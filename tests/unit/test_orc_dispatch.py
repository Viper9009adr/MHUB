"""Tests for the orc dispatch layer."""

from __future__ import annotations

import asyncio
import pytest

from src.agentic_cli.contracts import RunCommand, RunReceipt
from src.agentic_cli.service import AgenticCliService
from src.orc.dispatch import OrcDispatch, OrcDispatchError
from src.orc.task_queue import TaskItem, TaskQueue


class TestOrcDispatch:
    def test_dispatch_run_sync_execution(self) -> None:
        """Verify dispatch_run enqueues and executes synchronously."""
        dispatch = OrcDispatch()
        receipt = asyncio.run(
            dispatch.dispatch_run(prompt="test prompt")
        )
        assert receipt.session_id == "session-1"
        assert receipt.turn_index == 1
        assert receipt.backend == "local"
        assert receipt.output == "local:1:test prompt"

    def test_dispatch_run_with_session_id(self) -> None:
        dispatch = OrcDispatch()
        receipt = asyncio.run(
            dispatch.dispatch_run(prompt="hello", session_id="thread-5")
        )
        assert receipt.session_id == "thread-5"
        assert receipt.turn_index == 1

    def test_dispatch_run_with_metadata(self) -> None:
        dispatch = OrcDispatch()
        receipt = asyncio.run(
            dispatch.dispatch_run(prompt="hi", metadata={"env": "test"})
        )
        assert receipt.turn_index == 1

    def test_dispatch_enqueues_task(self) -> None:
        dispatch = OrcDispatch()
        asyncio.run(
            dispatch.dispatch_run(prompt="test")
        )
        assert dispatch.queue.processed_count == 1

    def test_dispatch_multiple_runs_increment_turns(self) -> None:
        service = AgenticCliService()
        dispatch = OrcDispatch(service=service)
        r1 = asyncio.run(dispatch.dispatch_run(prompt="first", session_id="s1"))
        r2 = asyncio.run(dispatch.dispatch_run(prompt="second", session_id="s1"))

        assert r1.turn_index == 1
        assert r2.turn_index == 2
        assert dispatch.queue.processed_count == 2

    def test_execute_missing_prompt_raises(self) -> None:
        dispatch = OrcDispatch()
        item = TaskItem(task_id="bad", payload={})
        with pytest.raises(OrcDispatchError, match="missing valid prompt"):
            dispatch._execute(item)

    def test_execute_invalid_prompt_type_raises(self) -> None:
        dispatch = OrcDispatch()
        item = TaskItem(task_id="bad", payload={"prompt": 123})
        with pytest.raises(OrcDispatchError, match="missing valid prompt"):
            dispatch._execute(item)

    def test_queue_property(self) -> None:
        dispatch = OrcDispatch()
        assert isinstance(dispatch.queue, TaskQueue)
