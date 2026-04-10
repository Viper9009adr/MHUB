"""gRPC client for the Meridian HubService.

Provides async helpers for TUI integration:
- create_hub: create a new hub and return its hub_id
- stream_prompt: stream a prompt through AgentStream, encoding hub_id in agent_id
- hub_status: retrieve the current state of a hub
- close: close the channel
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from typing import Any

import grpc

from src.hub.hub_pb2 import (
    AgentMessage,
    CreateHubRequest,
    HubStatusRequest,
)
from src.hub.hub_pb2_grpc import HubServiceStub

logger = logging.getLogger(__name__)

_DEFAULT_ADDRESS = "localhost:50052"


class GrpcHubClient:
    """Async gRPC client wrapping HubService stubs for TUI use.

    Args:
        address: gRPC server address in the form "host:port".
    """

    def __init__(self, address: str = _DEFAULT_ADDRESS) -> None:
        self._address = address
        self._channel: grpc.aio.Channel | None = None
        self._stub: HubServiceStub | None = None
        self._closed = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _ensure_channel(self) -> HubServiceStub:
        """Return the stub, creating the channel on first call."""
        if self._channel is None:
            self._channel = grpc.aio.insecure_channel(self._address)
            self._stub = HubServiceStub(self._channel)
        assert self._stub is not None
        return self._stub

    async def close(self) -> None:
        """Close the gRPC channel."""
        self._closed = True
        if self._channel is not None:
            await self._channel.close()
            self._channel = None
            self._stub = None

    # ------------------------------------------------------------------
    # RPC wrappers
    # ------------------------------------------------------------------

    async def create_hub(self, workspace_id: str, initiator: str) -> str:
        """Create a new hub and return its hub_id.

        Args:
            workspace_id: Workspace identifier for the new hub.
            initiator: Identifier of the initiating agent or user.

        Returns:
            hub_id string assigned by the server.
        """
        stub = self._ensure_channel()
        response = await stub.CreateHub(
            CreateHubRequest(workspace_id=workspace_id, initiator=initiator),
            timeout=5.0,
        )
        logger.debug("create_hub: hub_id=%s", response.hub_id)
        return response.hub_id

    async def stream_prompt(
        self,
        hub_id: str,
        run_id: int,
        prompt: str,
    ) -> AsyncIterator[str]:
        """Stream a prompt through AgentStream.

        The hub_id is encoded into the agent_id field as "hub:<hub_id>:cli"
        so the server-side router can associate the stream with the correct hub.

        Args:
            hub_id: Hub identifier returned by create_hub.
            run_id: Integer run identifier for this streaming session.
            prompt: The prompt text to send.

        Yields:
            Decoded string chunks from the server response stream.
        """
        stub = self._ensure_channel()
        agent_id = f"hub:{hub_id}:agent:cli"

        async def _request_gen() -> AsyncIterator[AgentMessage]:
            yield AgentMessage(
                run_id=run_id,
                agent_id=agent_id,
                payload=prompt.encode("utf-8"),
                seq=0,
                done=False,
                err="",
            )
            # Signal end of client stream
            yield AgentMessage(
                run_id=run_id,
                agent_id=agent_id,
                payload=b"",
                seq=1,
                done=True,
                err="",
            )

        try:
            async for response in stub.AgentStream(_request_gen()):
                if response.err:
                    logger.error("stream_prompt error: %s", response.err)
                    break
                if response.done:
                    break
                if response.payload:
                    yield response.payload.decode("utf-8", errors="replace")
        except grpc.aio.AioRpcError as exc:
            logger.error("stream_prompt gRPC error: %s", exc)
            raise

    async def hub_status(self, hub_id: str) -> str:
        """Retrieve the current state string for a hub.

        Args:
            hub_id: Hub identifier to query.

        Returns:
            State string (e.g. "active", "terminated", "unknown").
        """
        stub = self._ensure_channel()
        try:
            response = await stub.HubStatus(HubStatusRequest(hub_id=hub_id), timeout=5.0)
            logger.debug("hub_status: hub_id=%s state=%s", hub_id, response.state)
            return response.state
        except (grpc.aio.AioRpcError, asyncio.TimeoutError) as exc:
            logger.warning("hub_status gRPC error: %s", exc)
            return "unknown"

    async def listen_orc_events(self) -> "AsyncIterator[tuple[str, str, str]]":
        """Subscribe to OrchestratorEvents RPC and yield (type, status, payload) tuples.

        Opens a bidirectional OrchestratorEvents stream, sends an initial
        subscribe frame, then yields decoded frames until the stream closes or
        an error occurs.

        Yields:
            (type_str, status_str, payload_str) for each incoming frame.
            On AioRpcError yields ("ERR", "", str(e)) then returns.
        """
        from src.hub.hub_pb2 import OrchEvent

        stub = self._ensure_channel()
        stop = asyncio.Event()

        async def _subscribe_gen() -> "AsyncIterator[OrchEvent]":
            yield OrchEvent(type="subscribe", payload=b"cli-hub-view")
            await stop.wait()

        try:
            async for event in stub.OrchestratorEvents(_subscribe_gen()):
                if self._closed:
                    return
                type_str = event.type or ""
                status_str = event.status or ""
                payload_str = event.payload.decode("utf-8", errors="replace") if event.payload else ""
                yield (type_str, status_str, payload_str)
        except grpc.aio.AioRpcError as exc:
            if self._closed:
                return
            logger.warning("listen_orc_events gRPC error: %s", exc)
            yield ("ERR", "", str(exc))
            return
        finally:
            stop.set()


__all__ = ["GrpcHubClient"]
