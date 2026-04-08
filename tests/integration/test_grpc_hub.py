"""Integration tests for the gRPC Hub service using in-process server."""

from __future__ import annotations

import asyncio
import json
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
    AgentMessage,
    OrchEvent,
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


@pytest.mark.asyncio
async def test_orchestrator_events_rejects_non_hub_meta_and_never_calls_llm() -> None:
    class _Provider:
        async def complete(self, prompt: str, model: str):
            raise AssertionError("LLM should not run on pre-LLM gate miss")

        async def stream(self, prompt: str, model: str):
            raise AssertionError("LLM stream should not run on pre-LLM gate miss")
            yield

    server = grpc.aio.server()
    hub_service = HubService(llm_provider=_Provider(), llm_model="test-model")
    add_HubServiceServicer_to_server(hub_service, server)
    port = server.add_insecure_port("[::]:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"localhost:{port}")
    stub = HubServiceStub(channel)

    async def _sub() -> AsyncIterator[OrchEvent]:
        yield OrchEvent(type="subscribe", payload=b"integration")
        await asyncio.sleep(0.2)

    async def _agent_req() -> AsyncIterator[AgentMessage]:
        yield AgentMessage(run_id=9, agent_id="hub:h1:agent:cli", payload=b"hello", seq=0, done=False, err="")
        yield AgentMessage(run_id=9, agent_id="hub:h1:agent:cli", payload=b"", seq=1, done=True, err="")

    try:
        event_stream = stub.OrchestratorEvents(_sub())
        event_task = asyncio.create_task(event_stream.__anext__())
        responses = [item async for item in stub.AgentStream(_agent_req())]
        event = await event_task
        assert (event.type, event.status) == ("HUB", "reject")
        assert responses[0].agent_id == "HubStatus"
        assert responses[0].payload.decode("utf-8").startswith("REJECTED:")
    finally:
        await channel.close()
        await server.stop(grace=0)


@pytest.mark.asyncio
async def test_agent_stream_hub_location_bypasses_llm_over_grpc() -> None:
    class _Provider:
        async def complete(self, prompt: str, model: str):
            raise AssertionError("LLM should not run for location")

        async def stream(self, prompt: str, model: str):
            raise AssertionError("LLM stream should not run for location")
            yield

    server = grpc.aio.server()
    hub_service = HubService(llm_provider=_Provider(), llm_model="test-model")
    add_HubServiceServicer_to_server(hub_service, server)
    port = server.add_insecure_port("[::]:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"localhost:{port}")
    stub = HubServiceStub(channel)

    create_resp = await stub.CreateHub(
        CreateHubRequest(workspace_id="ws-1", initiator="user-1")
    )

    async def _agent_req() -> AsyncIterator[AgentMessage]:
        yield AgentMessage(
            run_id=21,
            agent_id=f"hub:{create_resp.hub_id}:agent:cli",
            payload=b"location",
            seq=0,
            done=False,
            err="",
        )
        yield AgentMessage(
            run_id=21,
            agent_id=f"hub:{create_resp.hub_id}:agent:cli",
            payload=b"",
            seq=1,
            done=True,
            err="",
        )

    try:
        responses = [item async for item in stub.AgentStream(_agent_req())]
        assert responses[0].agent_id == "HubStatus"
        assert responses[0].payload.decode("utf-8") == create_resp.hub_id
    finally:
        await channel.close()
        await server.stop(grace=0)


@pytest.mark.asyncio
async def test_agent_stream_hub_id_form_maps_to_hubstatus_location() -> None:
    class _Provider:
        async def complete(self, prompt: str, model: str):
            raise AssertionError("LLM should not run for hub-id")

        async def stream(self, prompt: str, model: str):
            raise AssertionError("LLM stream should not run for hub-id")
            yield

    server = grpc.aio.server()
    hub_service = HubService(llm_provider=_Provider(), llm_model="test-model")
    add_HubServiceServicer_to_server(hub_service, server)
    port = server.add_insecure_port("[::]:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"localhost:{port}")
    stub = HubServiceStub(channel)

    create_resp = await stub.CreateHub(
        CreateHubRequest(workspace_id="ws-1", initiator="user-1")
    )

    async def _agent_req() -> AsyncIterator[AgentMessage]:
        yield AgentMessage(
            run_id=24,
            agent_id=f"hub:{create_resp.hub_id}:agent:cli",
            payload=b"hub-id",
            seq=0,
            done=False,
            err="",
        )
        yield AgentMessage(
            run_id=24,
            agent_id=f"hub:{create_resp.hub_id}:agent:cli",
            payload=b"",
            seq=1,
            done=True,
            err="",
        )

    try:
        responses = [item async for item in stub.AgentStream(_agent_req())]
        assert responses[0].agent_id == "HubStatus"
        assert responses[0].payload.decode("utf-8") == create_resp.hub_id
    finally:
        await channel.close()
        await server.stop(grace=0)


@pytest.mark.asyncio
async def test_agent_stream_hub_report_no_hub_error_over_grpc() -> None:
    class _Provider:
        async def complete(self, prompt: str, model: str):
            raise AssertionError("LLM should not run for report")

        async def stream(self, prompt: str, model: str):
            raise AssertionError("LLM stream should not run for report")
            yield

    server = grpc.aio.server()
    hub_service = HubService(llm_provider=_Provider(), llm_model="test-model")
    add_HubServiceServicer_to_server(hub_service, server)
    port = server.add_insecure_port("[::]:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"localhost:{port}")
    stub = HubServiceStub(channel)

    async def _agent_req() -> AsyncIterator[AgentMessage]:
        yield AgentMessage(
            run_id=22,
            agent_id="hub:missing-hub:agent:cli",
            payload=b"report",
            seq=0,
            done=False,
            err="",
        )

    try:
        responses = [item async for item in stub.AgentStream(_agent_req())]
        assert responses[0].agent_id == "HubStatus"
        assert responses[0].err == "nohub"
    finally:
        await channel.close()
        await server.stop(grace=0)


@pytest.mark.asyncio
async def test_agent_stream_status_uses_hubstatus_and_emits_status_check() -> None:
    class _Provider:
        async def complete(self, prompt: str, model: str):
            raise AssertionError("LLM should not run for status")

        async def stream(self, prompt: str, model: str):
            raise AssertionError("LLM stream should not run for status")
            yield

    server = grpc.aio.server()
    hub_service = HubService(llm_provider=_Provider(), llm_model="test-model")
    add_HubServiceServicer_to_server(hub_service, server)
    port = server.add_insecure_port("[::]:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"localhost:{port}")
    stub = HubServiceStub(channel)

    create_resp = await stub.CreateHub(
        CreateHubRequest(workspace_id="ws-1", initiator="user-1")
    )

    async def _sub() -> AsyncIterator[OrchEvent]:
        yield OrchEvent(type="subscribe", payload=b"integration")
        await asyncio.sleep(0.2)

    async def _agent_req() -> AsyncIterator[AgentMessage]:
        yield AgentMessage(
            run_id=23,
            agent_id=f"hub:{create_resp.hub_id}:agent:cli",
            payload=b"status",
            seq=0,
            done=False,
            err="",
        )

    try:
        event_stream = stub.OrchestratorEvents(_sub())
        event_task = asyncio.create_task(event_stream.__anext__())
        responses = [item async for item in stub.AgentStream(_agent_req())]
        event = await event_task
        payload = json.loads(responses[0].payload.decode("utf-8"))
        assert responses[0].agent_id == "HubStatus"
        assert payload == {"id": create_resp.hub_id, "current": "active"}
        assert (event.type, event.status) == ("HUB", "status_check")
    finally:
        await channel.close()
        await server.stop(grace=0)
