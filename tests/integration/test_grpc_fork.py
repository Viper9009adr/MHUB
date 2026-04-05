"""Integration tests for gRPC fork detection and decision flow."""

from __future__ import annotations

from collections.abc import AsyncIterator

import grpc
import pytest
import pytest_asyncio

from src.hub.hub_pb2 import DecideForkRequest, DetectForkRequest
from src.hub.hub_pb2_grpc import HubServiceStub, add_HubServiceServicer_to_server
from src.hub.service import HubService
from src.orc.fork_handler import ForkHandler
from src.storage.models import HubRecord
from src.storage.null_backend import NullStorageBackend


@pytest_asyncio.fixture
async def fork_grpc_env() -> AsyncIterator[tuple[grpc.aio.Channel, NullStorageBackend]]:
    """Start an in-process gRPC server with shared fork dependencies."""
    backend = NullStorageBackend()
    handler = ForkHandler(storage=backend, redis=None)
    hub_service = HubService(storage=backend, fork_handler=handler)

    server = grpc.aio.server()
    add_HubServiceServicer_to_server(hub_service, server)
    port = server.add_insecure_port("[::]:0")
    await server.start()

    channel = grpc.aio.insecure_channel(f"localhost:{port}")
    try:
        yield channel, backend
    finally:
        await channel.close()
        await server.stop(grace=0)


@pytest.mark.asyncio
async def test_grpc_detect_then_approve_spawns_new_hub(
    fork_grpc_env: tuple[grpc.aio.Channel, NullStorageBackend],
) -> None:
    """DetectFork + DecideFork('approve') creates a retrievable child hub."""
    channel, backend = fork_grpc_env
    stub = HubServiceStub(channel)

    # Seed parent hub in shared backend used by ForkHandler.
    await backend.insert_hub(
        HubRecord(hub_id="hub-parent", workspace_id="ws-1", initiator="alice")
    )

    detect_resp = await stub.DetectFork(
        DetectForkRequest(
            parent_hub_id="hub-parent",
            hal_agent_id="hal-007",
            divergence_type="state_mismatch",
            divergence_reason="counter drifted",
            divergence_payload=b"proof",
        )
    )

    assert detect_resp.fork_id
    assert detect_resp.status == "pending"
    pending = await backend.get_fork_scenario(detect_resp.fork_id)
    assert pending is not None
    assert pending.parent_hub_id == "hub-parent"
    assert pending.status == "pending"
    assert pending.user_decision is None

    decide_resp = await stub.DecideFork(
        DecideForkRequest(fork_id=detect_resp.fork_id, decision="approve")
    )

    assert decide_resp.ok is True
    assert decide_resp.new_hub_id
    created = await backend.get_hub(decide_resp.new_hub_id)
    assert created is not None
    assert created.workspace_id == "ws-1"
    assert created.initiator == "alice"
    assert created.state == "active"

    decided = await backend.get_fork_scenario(detect_resp.fork_id)
    assert decided is not None
    assert decided.status == "approved"
    assert decided.user_decision == "approve"
    assert decided.new_hub_id == decide_resp.new_hub_id
    assert decided.decided_at is not None


@pytest.mark.asyncio
async def test_grpc_detect_then_reject_persists_rejected_decision(
    fork_grpc_env: tuple[grpc.aio.Channel, NullStorageBackend],
) -> None:
    """DetectFork + DecideFork('reject') marks scenario rejected without child hub."""
    channel, backend = fork_grpc_env
    stub = HubServiceStub(channel)

    await backend.insert_hub(
        HubRecord(hub_id="hub-parent", workspace_id="ws-1", initiator="alice")
    )

    detect_resp = await stub.DetectFork(
        DetectForkRequest(
            parent_hub_id="hub-parent",
            hal_agent_id="hal-007",
            divergence_type="state_mismatch",
            divergence_reason="counter drifted",
            divergence_payload=b"proof",
        )
    )
    decide_resp = await stub.DecideFork(
        DecideForkRequest(fork_id=detect_resp.fork_id, decision="reject")
    )

    assert decide_resp.ok is True
    assert decide_resp.new_hub_id == ""
    decided = await backend.get_fork_scenario(detect_resp.fork_id)
    assert decided is not None
    assert decided.status == "rejected"
    assert decided.user_decision == "reject"
    assert decided.new_hub_id is None
    assert decided.decided_at is not None
