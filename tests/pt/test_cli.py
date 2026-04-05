from __future__ import annotations

from io import StringIO

from src.agentic_cli.cli import main
from src.agentic_cli.memory import InMemoryConversationStore
from src.agentic_cli.service import AgenticCliService


def test_run_command_emits_receipt_for_success() -> None:
    out = StringIO()

    code = main(["run", "ship it"], out=out)

    assert code == 0
    assert out.getvalue().strip() == "session_id=session-1 turn_index=1 backend=local output=local:1:ship it"


def test_run_command_reuses_injected_service_state() -> None:
    out = StringIO()
    service = AgenticCliService(memory=InMemoryConversationStore())

    first = main(["run", "first", "--session-id", "thread-3"], out=out, service=service)
    second = main(["run", "second", "--session-id", "thread-3"], out=out, service=service)

    assert first == 0
    assert second == 0
    lines = out.getvalue().strip().splitlines()
    assert lines == [
        "session_id=thread-3 turn_index=1 backend=local output=local:1:first",
        "session_id=thread-3 turn_index=2 backend=local output=local:2:second",
    ]


def test_run_command_returns_two_for_contract_error() -> None:
    out = StringIO()

    code = main(["run", "hello", "--session-id", "   "], out=out)

    assert code == 2
    assert out.getvalue().strip() == "error: session_id must not be blank"


def test_run_command_returns_two_for_missing_prompt() -> None:
    out = StringIO()

    code = main(["run"], out=out)

    assert code == 2
    assert "the following arguments are required: prompt" in out.getvalue()
