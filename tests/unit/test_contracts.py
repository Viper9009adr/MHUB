from __future__ import annotations

import pytest

from src.agentic_cli.contracts import RunCommand, RunReceipt, format_run_receipt
from src.agentic_cli.errors import AgenticCliError, BackendExecutionError, ContractViolationError


def test_run_command_rejects_blank_prompt() -> None:
    with pytest.raises(ContractViolationError, match="prompt must not be blank"):
        RunCommand(prompt="   ")


def test_run_command_rejects_blank_session_id() -> None:
    with pytest.raises(ContractViolationError, match="session_id must not be blank"):
        RunCommand(prompt="ok", session_id="   ")


def test_run_command_rejects_non_string_metadata_values() -> None:
    with pytest.raises(ContractViolationError, match="metadata value must be a string"):
        RunCommand(prompt="ok", metadata={"mode": 1})


def test_format_run_receipt_is_stable() -> None:
    receipt = RunReceipt(session_id="session-1", turn_index=2, backend="local", output="local:2:done")

    assert (
        format_run_receipt(receipt)
        == "session_id=session-1 turn_index=2 backend=local output=local:2:done"
    )


def test_error_types_share_common_base() -> None:
    assert issubclass(ContractViolationError, AgenticCliError)
    assert issubclass(BackendExecutionError, AgenticCliError)
