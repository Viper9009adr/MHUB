from __future__ import annotations

from src.agentic_cli.contracts import RunCommand
from src.agentic_cli.memory import InMemoryConversationStore
from src.agentic_cli.service import AgenticCliService


def test_service_allocates_session_id_for_new_run() -> None:
    service = AgenticCliService()

    receipt = service.run(RunCommand(prompt="draft a summary"))

    assert receipt.session_id == "session-1"
    assert receipt.turn_index == 1
    assert receipt.backend == "local"
    assert receipt.output == "local:1:draft a summary"


def test_service_reuses_explicit_session_id_and_increments_turn_index() -> None:
    service = AgenticCliService()

    first = service.run(RunCommand(prompt="first", session_id="thread-7"))
    second = service.run(RunCommand(prompt="follow up", session_id="thread-7"))

    assert first.session_id == "thread-7"
    assert first.turn_index == 1
    assert second.session_id == "thread-7"
    assert second.turn_index == 2
    assert second.output == "local:2:follow up"


def test_service_persists_turn_history_in_memory_store() -> None:
    memory = InMemoryConversationStore()
    service = AgenticCliService(memory=memory)

    service.run(RunCommand(prompt="first", session_id="thread-9"))
    service.run(RunCommand(prompt="second", session_id="thread-9"))

    history = memory.read_history("thread-9")
    assert [turn.prompt for turn in history] == ["first", "second"]
    assert [turn.turn_index for turn in history] == [1, 2]
