from __future__ import annotations

import pytest

from src.agentic_cli.grpc_client import GrpcHubClient
from src.hub.hub_pb2 import AgentMessage, OrchEvent


@pytest.mark.asyncio
async def test_listen_orc_events_decodes_triples(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Stub:
        async def OrchestratorEvents(self, request_iter):
            async for _ in request_iter:
                break
            yield OrchEvent(type="ARC", status="plan", payload=b"- step")

    client = GrpcHubClient()
    monkeypatch.setattr(client, "_ensure_channel", lambda: _Stub())

    received = []
    async for event in client.listen_orc_events():
        received.append(event)
        break

    assert received == [("ARC", "plan", "- step")]


@pytest.mark.asyncio
async def test_stream_prompt_ignores_terminal_ack_frame(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Stub:
        async def AgentStream(self, request_iter):
            async for _ in request_iter:
                pass
            yield AgentMessage(run_id=1, agent_id="ORC", payload=b"final", seq=1, done=False, err="")
            yield AgentMessage(run_id=1, agent_id="orchestrator", payload=b"ack", seq=2, done=True, err="")

    client = GrpcHubClient()
    monkeypatch.setattr(client, "_ensure_channel", lambda: _Stub())

    chunks = []
    async for chunk in client.stream_prompt(hub_id="h1", run_id=1, prompt="hello"):
        chunks.append(chunk)

    assert chunks == ["final"]
