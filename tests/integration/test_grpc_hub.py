"""Integration tests for the gRPC Hub service using in-process server."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import grpc
import pytest
import pytest_asyncio

from src.hub.hub_pb2 import (
    CreateHubRequest,
    TerminateHubRequest,
    JoinHubRequest,
    TapHubRequest,
    CreateCheckpointRequest,
    RollbackCheckpointRequest,
    HubStatusRequest,
)
from src.hub.hub_pb2_grpc import HubServiceStub, add_HubServiceServicer_to_server
from src.hub.service import HubService


@pytest_asyncio.fixture
async def grpc_channel() -> AsyncIterator[grpc.aio.Channel]:
    """Start an in-process gRPC server and yield a channel."""
    server = grpc.aio.server()
    hub_service = HubService()
    add_HubServiceServicer_to_server(hub_service, server)
    port = server.add_insecure_port("[::]:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"localhost:{port}")
    try:
        yield channel
    finally:
        await channel.close()
        await server.stop(grace=0)


@pytest.mark.asyncio
async def test_create_hub(grpc_channel: grpc.aio.Channel) -> None:
    """Test creating a hub via gRPC."""
    stub = HubServiceStub(grpc_channel)
    request = CreateHubRequest(workspace_id="ws-1", initiator="user-1")
    response = await stub.CreateHub(request)
    assert response.hub_id
    assert response.created_at > 0


@pytest.mark.asyncio
async def test_terminate_hub(grpc_channel: grpc.aio.Channel) -> None:
    """Test terminating a hub via gRPC."""
    stub = HubServiceStub(grpc_channel)
    create_resp = await stub.CreateHub(
        CreateHubRequest(workspace_id="ws-1", initiator="user-1")
    )
    response = await stub.TerminateHub(
        TerminateHubRequest(hub_id=create_resp.hub_id, reason="test")
    )
    assert response.ok is True


@pytest.mark.asyncio
async def test_terminate_hub_not_found(grpc_channel: grpc.aio.Channel) -> None:
    """Test terminating a non-existent hub returns NOT_FOUND."""
    stub = HubServiceStub(grpc_channel)
    try:
        await stub.TerminateHub(
            TerminateHubRequest(hub_id="nonexistent", reason="test")
        )
        pytest.fail("Expected grpc.RpcError")
    except grpc.aio.AioRpcError as exc:
        assert exc.code() == grpc.StatusCode.NOT_FOUND


@pytest.mark.asyncio
async def test_join_hub_streaming(grpc_channel: grpc.aio.Channel) -> None:
    """Test bidirectional streaming JoinHub."""
    stub = HubServiceStub(grpc_channel)
    create_resp = await stub.CreateHub(
        CreateHubRequest(workspace_id="ws-1", initiator="user-1")
    )

    async def request_gen() -> AsyncIterator[JoinHubRequest]:
        for i in range(3):
            yield JoinHubRequest(
                hub_id=create_resp.hub_id,
                member_id=f"member-{i}",
                event="join",
            )

    events = []
    async for event in stub.JoinHub(request_gen()):
        events.append(event)

    assert len(events) == 3
    assert events[0].member_id == "member-0"
    assert events[0].hub_id == create_resp.hub_id


@pytest.mark.asyncio
async def test_tap_hub(grpc_channel: grpc.aio.Channel) -> None:
    """Test tapping into hub events."""
    stub = HubServiceStub(grpc_channel)
    create_resp = await stub.CreateHub(
        CreateHubRequest(workspace_id="ws-1", initiator="user-1")
    )

    # Generate some events via JoinHub
    async def request_gen() -> AsyncIterator[JoinHubRequest]:
        for i in range(5):
            yield JoinHubRequest(
                hub_id=create_resp.hub_id,
                member_id=f"member-{i}",
                event="join",
            )

    async for _ in stub.JoinHub(request_gen()):
        pass

    response = await stub.TapHub(
        TapHubRequest(hub_id=create_resp.hub_id, limit=2)
    )
    assert len(response.events) == 2


@pytest.mark.asyncio
async def test_create_and_rollback_checkpoint(grpc_channel: grpc.aio.Channel) -> None:
    """Test checkpoint creation and rollback."""
    stub = HubServiceStub(grpc_channel)
    create_resp = await stub.CreateHub(
        CreateHubRequest(workspace_id="ws-1", initiator="user-1")
    )

    cp_resp = await stub.CreateCheckpoint(
        CreateCheckpointRequest(hub_id=create_resp.hub_id, label="v1")
    )
    assert cp_resp.checkpoint_id

    rollback_resp = await stub.RollbackCheckpoint(
        RollbackCheckpointRequest(
            hub_id=create_resp.hub_id, checkpoint_id=cp_resp.checkpoint_id
        )
    )
    assert rollback_resp.ok is True


@pytest.mark.asyncio
async def test_hub_status(grpc_channel: grpc.aio.Channel) -> None:
    """Test getting hub status."""
    stub = HubServiceStub(grpc_channel)
    create_resp = await stub.CreateHub(
        CreateHubRequest(workspace_id="ws-1", initiator="user-1")
    )

    response = await stub.HubStatus(
        HubStatusRequest(hub_id=create_resp.hub_id)
    )
    assert response.hub_id == create_resp.hub_id
    assert response.state == "active"


@pytest.mark.asyncio
async def test_hub_status_not_found(grpc_channel: grpc.aio.Channel) -> None:
    """Test getting status for non-existent hub."""
    stub = HubServiceStub(grpc_channel)
    try:
        await stub.HubStatus(HubStatusRequest(hub_id="nonexistent"))
        pytest.fail("Expected grpc.RpcError")
    except grpc.aio.AioRpcError as exc:
        assert exc.code() == grpc.StatusCode.NOT_FOUND
