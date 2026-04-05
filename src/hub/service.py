"""gRPC HubService servicer implementation."""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import time
import uuid
from collections import defaultdict
from collections.abc import Iterator
from typing import Any

import grpc

from src.hub.hub_pb2 import (
    CreateHubRequest,
    CreateHubResponse,
    TerminateHubRequest,
    TerminateHubResponse,
    JoinHubRequest,
    JoinHubEvent,
    TapHubRequest,
    TapHubResponse,
    CreateCheckpointRequest,
    CreateCheckpointResponse,
    RollbackCheckpointRequest,
    RollbackCheckpointResponse,
    HubStatusRequest,
    HubStatusResponse,
    DetectForkRequest,
    DetectForkResponse,
    DecideForkRequest,
    DecideForkResponse,
    AgentMessage,
    OrchEvent,
)
from src.hub.hub_pb2_grpc import HubServiceServicer
from src.hub.stream_router import StreamRouter
from src.storage.models import HubRecord, CheckpointRecord, ForkScenarioRecord
from src.orc.fork_handler import ForkHandler, DivergenceEvent

logger = logging.getLogger(__name__)


class HubService(HubServiceServicer):
    """Concrete implementation of the HubService gRPC servicer.

    When a storage backend is provided, hub and checkpoint data is
    persisted through it. Otherwise, in-memory dicts are used as a
    fallback (original behaviour).

    A ``StreamRouter`` singleton is created on construction and used by
    ``AgentStream`` to register, broadcast to, and unregister per-run
    outbound queues.  When an LLM provider is supplied, ``AgentStream``
    forwards decoded payloads to ``llm_provider.stream()`` and yields the
    response chunks back to the caller.  Without a provider, payloads are
    echoed verbatim.
    """

    def __init__(
        self,
        storage: Any | None = None,
        fork_handler: ForkHandler | None = None,
        llm_provider: Any = None,
        llm_model: str = "",
    ) -> None:
        """Initialise HubService.

        Args:
            storage: Optional async storage backend for hub/checkpoint
                persistence.  When ``None``, in-memory dicts are used.
            fork_handler: Optional ``ForkHandler`` for DetectFork /
                DecideFork RPCs.  When ``None`` those RPCs return
                ``UNAVAILABLE``.
            llm_provider: Optional LLM provider instance.  Must expose an
                ``async def stream(prompt, model) -> AsyncGenerator[LLMChunk, None]``
                interface.  When ``None``, ``AgentStream`` echoes payloads.
        """
        self._storage = storage
        self._fork_handler = fork_handler
        self._llm_provider = llm_provider
        self._llm_model = llm_model
        self._stream_router = StreamRouter()
        self._hubs: dict[str, dict[str, Any]] = {}
        self._events: dict[str, list[JoinHubEvent]] = defaultdict(list)
        self._checkpoints: dict[str, dict[str, str]] = defaultdict(dict)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _has_storage(self) -> bool:
        return self._storage is not None

    def _get_event_loop(self) -> asyncio.AbstractEventLoop:
        """Get an event loop, compatible with both running and non-running contexts.

        Uses get_event_loop() with try/except fallback to avoid RuntimeError
        in test environments without a running loop.
        """
        try:
            return asyncio.get_event_loop()
        except RuntimeError:
            return asyncio.new_event_loop()

    def _schedule_coroutine(self, loop: asyncio.AbstractEventLoop, coro: Any) -> None:
        """Schedule a coroutine safely from a synchronous context.

        Distinguishes two cases:
        - Same thread as the loop (e.g., pytest-asyncio, grpc.aio): run the
          coroutine in a background thread with its own event loop via
          asyncio.run(). Cannot use ensure_future (fire-and-forget) because
          the caller's next await may not yield the loop before reading the
          result, leaving the task unexecuted. Cannot block the current thread
          directly (would deadlock the running loop). ThreadPoolExecutor +
          asyncio.run() gives us synchronous completion without deadlock.
        - Different thread from loop (e.g., production gRPC worker threads):
          use run_coroutine_threadsafe + future.result() which is safe because
          blocking the worker thread does NOT block the loop thread.
        """
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

        if running_loop is loop:
            # Same thread as the running loop — spin up a background thread
            # with its own event loop so the coroutine completes synchronously
            # without touching the caller's running loop.
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                bg_future = ex.submit(asyncio.run, coro)
                try:
                    bg_future.result(timeout=5)
                except concurrent.futures.TimeoutError:
                    logger.error("storage op timed out (same-thread path)")
                except Exception as e:
                    logger.error("storage op failed (same-thread path): %s", e)
        else:
            # Different thread — safe to block the current thread.
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            try:
                future.result(timeout=5)
            except concurrent.futures.TimeoutError:
                logger.error("storage op timed out")
            except Exception as e:
                logger.error("storage op failed: %s", e)

    # ------------------------------------------------------------------
    # RPCs
    # ------------------------------------------------------------------

    def CreateHub(
        self, request: CreateHubRequest, context: grpc.ServicerContext
    ) -> CreateHubResponse:
        """Create a new hub instance."""
        hub_id = str(uuid.uuid4())
        created_at = int(time.time())

        if self._has_storage():
            record = HubRecord(
                hub_id=hub_id,
                workspace_id=request.workspace_id,
                initiator=request.initiator,
                created_at=created_at,
                state="active",
            )
            # Storage insert is async but gRPC servicer methods are sync.
            # Always update in-memory dict first, then persist to storage.
            self._hubs[hub_id] = {
                "workspace_id": request.workspace_id,
                "initiator": request.initiator,
                "created_at": created_at,
                "state": "active",
            }
            try:
                loop = self._get_event_loop()
                if loop.is_running():
                    self._schedule_coroutine(loop, self._storage.insert_hub(record))
                else:
                    loop.run_until_complete(self._storage.insert_hub(record))
            except RuntimeError:
                pass  # in-memory dict already updated above
        else:
            self._hubs[hub_id] = {
                "workspace_id": request.workspace_id,
                "initiator": request.initiator,
                "created_at": created_at,
                "state": "active",
            }

        return CreateHubResponse(hub_id=hub_id, created_at=created_at)

    def TerminateHub(
        self, request: TerminateHubRequest, context: grpc.ServicerContext
    ) -> TerminateHubResponse:
        """Terminate an existing hub."""
        if self._has_storage():
            try:
                loop = self._get_event_loop()
                if loop.is_running():
                    self._schedule_coroutine(
                        loop,
                        self._storage.update_hub_state(
                            request.hub_id, "terminated", request.reason
                        ),
                    )
                    # Update in-memory dict; fall through to dict logic below.
                else:
                    ok = loop.run_until_complete(
                        self._storage.update_hub_state(
                            request.hub_id, "terminated", request.reason
                        )
                    )
                    if not ok:
                        context.set_code(grpc.StatusCode.NOT_FOUND)
                        context.set_details(f"Hub {request.hub_id} not found")
                        return TerminateHubResponse(ok=False)
                    return TerminateHubResponse(ok=True)
            except RuntimeError:
                pass  # fall through to dict logic

        hub = self._hubs.get(request.hub_id)
        if hub is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Hub {request.hub_id} not found")
            return TerminateHubResponse(ok=False)
        hub["state"] = "terminated"
        hub["terminated_reason"] = request.reason
        return TerminateHubResponse(ok=True)

    def JoinHub(
        self, request_iterator: Iterator[JoinHubRequest], context: grpc.ServicerContext
    ) -> Iterator[JoinHubEvent]:
        """Bidirectional streaming for hub membership events."""
        for req in request_iterator:
            event = JoinHubEvent(
                hub_id=req.hub_id,
                member_id=req.member_id,
                event=req.event,
                at=int(time.time()),
            )
            self._events[req.hub_id].append(event)
            yield event

    def TapHub(
        self, request: TapHubRequest, context: grpc.ServicerContext
    ) -> TapHubResponse:
        """Tap into hub event stream with a limit."""
        events = self._events.get(request.hub_id, [])
        limit = request.limit if request.limit > 0 else len(events)
        return TapHubResponse(events=events[:limit])

    def CreateCheckpoint(
        self, request: CreateCheckpointRequest, context: grpc.ServicerContext
    ) -> CreateCheckpointResponse:
        """Create a checkpoint for a hub."""
        # Verify hub exists
        if self._has_storage():
            try:
                loop = self._get_event_loop()
                if not loop.is_running():
                    hub_record = loop.run_until_complete(
                        self._storage.get_hub(request.hub_id)
                    )
                    if hub_record is None:
                        context.set_code(grpc.StatusCode.NOT_FOUND)
                        context.set_details(f"Hub {request.hub_id} not found")
                        return CreateCheckpointResponse(checkpoint_id="")
                else:
                    if request.hub_id not in self._hubs:
                        context.set_code(grpc.StatusCode.NOT_FOUND)
                        context.set_details(f"Hub {request.hub_id} not found")
                        return CreateCheckpointResponse(checkpoint_id="")
            except RuntimeError:
                if request.hub_id not in self._hubs:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details(f"Hub {request.hub_id} not found")
                    return CreateCheckpointResponse(checkpoint_id="")
        else:
            if request.hub_id not in self._hubs:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Hub {request.hub_id} not found")
                return CreateCheckpointResponse(checkpoint_id="")

        checkpoint_id = str(uuid.uuid4())

        if self._has_storage():
            record = CheckpointRecord(
                checkpoint_id=checkpoint_id,
                hub_id=request.hub_id,
                label=request.label,
            )
            try:
                loop = self._get_event_loop()
                if loop.is_running():
                    self._schedule_coroutine(
                        loop, self._storage.insert_checkpoint(record)
                    )
                else:
                    loop.run_until_complete(self._storage.insert_checkpoint(record))
            except RuntimeError:
                self._checkpoints[request.hub_id][checkpoint_id] = request.label
        else:
            self._checkpoints[request.hub_id][checkpoint_id] = request.label

        return CreateCheckpointResponse(checkpoint_id=checkpoint_id)

    def RollbackCheckpoint(
        self, request: RollbackCheckpointRequest, context: grpc.ServicerContext
    ) -> RollbackCheckpointResponse:
        """Rollback a hub to a checkpoint."""
        if self._has_storage():
            try:
                loop = self._get_event_loop()
                if not loop.is_running():
                    cp = loop.run_until_complete(
                        self._storage.get_checkpoint(request.checkpoint_id)
                    )
                    if cp is None:
                        context.set_code(grpc.StatusCode.NOT_FOUND)
                        context.set_details(
                            f"Checkpoint {request.checkpoint_id} not found"
                        )
                        return RollbackCheckpointResponse(ok=False)
                    return RollbackCheckpointResponse(ok=True)
            except RuntimeError:
                pass # fall through

        hub_checkpoints = self._checkpoints.get(request.hub_id, {})
        if request.checkpoint_id not in hub_checkpoints:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Checkpoint {request.checkpoint_id} not found")
            return RollbackCheckpointResponse(ok=False)
        return RollbackCheckpointResponse(ok=True)

    def HubStatus(
        self, request: HubStatusRequest, context: grpc.ServicerContext
    ) -> HubStatusResponse:
        """Get the current status of a hub."""
        if self._has_storage():
            try:
                loop = self._get_event_loop()
                if not loop.is_running():
                    hub_record = loop.run_until_complete(
                        self._storage.get_hub(request.hub_id)
                    )
                    if hub_record is None:
                        context.set_code(grpc.StatusCode.NOT_FOUND)
                        context.set_details(f"Hub {request.hub_id} not found")
                        return HubStatusResponse(
                            hub_id=request.hub_id, state="unknown", updated_at=0
                        )
                    return HubStatusResponse(
                        hub_id=request.hub_id,
                        state=hub_record.state,
                        updated_at=hub_record.created_at,
                    )
            except RuntimeError:
                pass # fall through

        hub = self._hubs.get(request.hub_id)
        if hub is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Hub {request.hub_id} not found")
            return HubStatusResponse(hub_id=request.hub_id, state="unknown", updated_at=0)
        return HubStatusResponse(
            hub_id=request.hub_id,
            state=hub["state"],
            updated_at=hub["created_at"],
        )

    def DetectFork(
        self, request: DetectForkRequest, context: grpc.ServicerContext
    ) -> DetectForkResponse:
        """Detect a fork divergence scenario.

        Delegates to ForkHandler for consistent processing across
        both Redis and gRPC paths.
        """
        if self._fork_handler is None:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("ForkHandler not available")
            return DetectForkResponse(fork_id="", status="error")

        event = DivergenceEvent(
            parent_hub_id=request.parent_hub_id,
            hal_agent_id=request.hal_agent_id,
            divergence_type=request.divergence_type,
            divergence_reason=request.divergence_reason,
            evidence=request.divergence_payload or b"",
            detected_at=int(time.time()),
        )

        try:
            loop = self._get_event_loop()
            if loop.is_running():
                self._schedule_coroutine(
                    loop, self._fork_handler.handle_detection(event)
                )
                return DetectForkResponse(fork_id="pending", status="detected")
            else:
                record: ForkScenarioRecord = loop.run_until_complete(
                    self._fork_handler.handle_detection(event)
                )
                return DetectForkResponse(
                    fork_id=record.fork_id,
                    status=record.status,
                )
        except Exception:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Failed to process fork detection")
            return DetectForkResponse(fork_id="", status="error")

    def DecideFork(
        self, request: DecideForkRequest, context: grpc.ServicerContext
    ) -> DecideForkResponse:
        """Submit a decision for a fork scenario.

        Delegates to ForkHandler for consistent processing across
        both Redis and gRPC paths.
        """
        if self._fork_handler is None:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("ForkHandler not available")
            return DecideForkResponse(ok=False)

        if request.decision not in ("approve", "reject"):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(
                f"Invalid decision: {request.decision!r}. Must be 'approve' or 'reject'."
            )
            return DecideForkResponse(ok=False)

        try:
            loop = self._get_event_loop()
            if loop.is_running():
                self._schedule_coroutine(
                    loop, self._fork_handler.process_decision(
                        request.fork_id, request.decision
                    )
                )
                return DecideForkResponse(ok=True, new_hub_id="pending")
            else:
                new_hub_id = loop.run_until_complete(
                    self._fork_handler.process_decision(
                        request.fork_id, request.decision
                    )
                )
                return DecideForkResponse(
                    ok=True,
                    new_hub_id=new_hub_id or "",
                )
        except Exception:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Failed to process fork decision")
            return DecideForkResponse(ok=False)

    # ------------------------------------------------------------------
    # Bidirectional streaming RPCs
    # ------------------------------------------------------------------

    async def AgentStream(
        self,
        request_iterator: Iterator[AgentMessage],
        context: grpc.ServicerContext,
    ) -> Iterator[AgentMessage]:
        """Bidirectional streaming for agent-to-agent communication.

        Agents send AgentMessage with run_id, agent_id, payload, seq, done, err.
        The hub_id is encoded in the agent_id field as "hub:<hub_id>:<agent_id>".
        When an LLM provider is configured, payload is forwarded to it and the
        response is streamed back. Falls back to echo when no provider is set.
        Termination: done=true or cancel signal.
        Reconnect on UNAVAILABLE.

        Args:
            request_iterator: Stream of AgentMessage from the client agent.
            context: gRPC servicer context.

        Yields:
            AgentMessage responses routed to the target agent.
        """
        outbound_queue: asyncio.Queue = asyncio.Queue()
        registered_run_id: int | None = None
        registered_agent_id: str | None = None

        async for msg in request_iterator:
            logger.debug(
                "AgentStream: run_id=%d agent_id=%s seq=%d done=%s",
                msg.run_id,
                msg.agent_id,
                msg.seq,
                msg.done,
            )

            # Register on first message
            if registered_run_id is None:
                registered_run_id = msg.run_id
                registered_agent_id = msg.agent_id
                if msg.agent_id.startswith("hub:"):
                    parts = msg.agent_id.split(":", 3)
                    hub_id = parts[1] if len(parts) > 1 else ""
                    agent_name = parts[3] if len(parts) > 3 else (parts[2] if len(parts) > 2 else "")
                    logger.debug("AgentStream: decoded hub_id=%s agent_name=%s", hub_id, agent_name)
                await self._stream_router.register_handler(
                    msg.run_id, msg.agent_id, outbound_queue
                )

            # Handle error messages
            if msg.err:
                logger.warning(
                    "AgentStream error: run_id=%d agent_id=%s err=%s",
                    msg.run_id,
                    msg.agent_id,
                    msg.err,
                )
                if registered_run_id is not None and registered_agent_id is not None:
                    await self._stream_router.unregister_handler(
                        registered_run_id, registered_agent_id
                    )
                yield AgentMessage(
                    run_id=msg.run_id,
                    agent_id="orchestrator",
                    payload=b"",
                    seq=msg.seq + 1,
                    done=True,
                    err=msg.err,
                )
                return

            # Handle termination
            if msg.done:
                logger.info(
                    "AgentStream done: run_id=%d agent_id=%s",
                    msg.run_id,
                    msg.agent_id,
                )
                if registered_run_id is not None and registered_agent_id is not None:
                    await self._stream_router.unregister_handler(
                        registered_run_id, registered_agent_id
                    )
                yield AgentMessage(
                    run_id=msg.run_id,
                    agent_id="orchestrator",
                    payload=b"ack",
                    seq=msg.seq + 1,
                    done=True,
                    err="",
                )
                return

            # Route or echo payload
            if self._llm_provider is not None:
                try:
                    prompt_text = msg.payload.decode("utf-8", errors="replace")
                    seq = msg.seq + 1
                    async for chunk in self._llm_provider.stream(prompt_text, model=self._llm_model):
                        chunk_bytes = chunk.text.encode("utf-8")
                        response_msg = AgentMessage(
                            run_id=msg.run_id,
                            agent_id="orchestrator",
                            payload=chunk_bytes,
                            seq=seq,
                            done=False,
                            err="",
                        )
                        await self._stream_router.broadcast(msg.run_id, response_msg)
                        yield response_msg
                        seq += 1
                except Exception as exc:
                    logger.error("LLM stream error: %s", exc)
                    if registered_run_id is not None and registered_agent_id is not None:
                        await self._stream_router.unregister_handler(
                            registered_run_id, registered_agent_id
                        )
                    yield AgentMessage(
                        run_id=msg.run_id,
                        agent_id="orchestrator",
                        payload=b"",
                        seq=msg.seq + 1,
                        done=True,
                        err=str(exc),
                    )
                    return
            else:
                # Echo fallback
                response_msg = AgentMessage(
                    run_id=msg.run_id,
                    agent_id="orchestrator",
                    payload=msg.payload,
                    seq=msg.seq + 1,
                    done=False,
                    err="",
                )
                await self._stream_router.broadcast(msg.run_id, response_msg)
                yield response_msg

        # Clean up registration if iterator exhausted without done=True
        if registered_run_id is not None and registered_agent_id is not None:
            await self._stream_router.unregister_handler(
                registered_run_id, registered_agent_id
            )

    async def OrchestratorEvents(
        self,
        request_iterator: Iterator[OrchEvent],
        context: grpc.ServicerContext,
    ) -> Iterator[OrchEvent]:
        """Bidirectional streaming for orchestrator events.

        Orchestrator sends OrchEvent with run_id, type, status, payload, ts.
        Used for real-time run status updates between orchestrator and agents.

        Args:
            request_iterator: Stream of OrchEvent from the client.
            context: gRPC servicer context.

        Yields:
            OrchEvent responses with acknowledgment.
        """
        for event in request_iterator:
            logger.debug(
                "OrchestratorEvents: run_id=%d type=%s status=%s",
                event.run_id,
                event.type,
                event.status,
            )

            # Acknowledge the event
            yield OrchEvent(
                run_id=event.run_id,
                type="ack",
                status=event.status,
                payload=b"",
                ts=int(time.time()),
            )
