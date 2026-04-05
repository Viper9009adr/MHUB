"""Unit tests for ForkHandler.handle_detection and process_decision."""

from __future__ import annotations

import pytest

from src.orc import fork_context
from src.orc.fork_handler import DivergenceEvent, ForkHandler
from src.storage.models import ForkScenarioRecord, HubRecord
from src.storage.null_backend import NullStorageBackend


@pytest.fixture(autouse=True)
def clear_fork_contexts() -> None:
    """Clear module-level fork context after every test."""
    yield
    fork_context._contexts.clear()


def _make_event(**kwargs) -> DivergenceEvent:
    defaults = {
        "parent_hub_id": "hub-parent",
        "hal_agent_id": "hal-007",
        "divergence_type": "state_mismatch",
        "divergence_reason": "counter drifted",
        "detected_at": 1000,
    }
    defaults.update(kwargs)
    return DivergenceEvent(**defaults)


# ---------------------------------------------------------------------------
# handle_detection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_detection_returns_fork_scenario_record() -> None:
    """handle_detection returns a ForkScenarioRecord on success."""
    backend = NullStorageBackend()
    handler = ForkHandler(storage=backend, redis=None)
    event = _make_event()

    record = await handler.handle_detection(event)

    assert isinstance(record, ForkScenarioRecord)
    assert record.parent_hub_id == "hub-parent"
    assert record.hal_agent_id == "hal-007"
    assert record.status == "pending"


@pytest.mark.asyncio
async def test_handle_detection_persists_record_in_backend() -> None:
    """handle_detection stores the record in the storage backend."""
    backend = NullStorageBackend()
    handler = ForkHandler(storage=backend, redis=None)
    event = _make_event()

    record = await handler.handle_detection(event)
    stored = await backend.get_fork_scenario(record.fork_id)

    assert stored is not None
    assert stored.fork_id == record.fork_id


@pytest.mark.asyncio
async def test_handle_detection_sets_fork_context() -> None:
    """handle_detection stores the event fields in the shared fork context."""
    backend = NullStorageBackend()
    handler = ForkHandler(storage=backend, redis=None)
    event = _make_event()

    record = await handler.handle_detection(event)
    ctx = fork_context.get_context(record.fork_id)

    assert ctx is not None
    assert ctx["parent_hub_id"] == "hub-parent"
    assert ctx["hal_agent_id"] == "hal-007"


@pytest.mark.asyncio
async def test_handle_detection_storage_failure_propagates() -> None:
    """handle_detection lets storage exceptions propagate to the caller."""
    from unittest.mock import AsyncMock

    failing_storage = AsyncMock()
    failing_storage.insert_fork_scenario = AsyncMock(side_effect=RuntimeError("db down"))
    handler = ForkHandler(storage=failing_storage, redis=None)
    event = _make_event()

    with pytest.raises(RuntimeError, match="db down"):
        await handler.handle_detection(event)


# ---------------------------------------------------------------------------
# process_decision
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_decision_approve_returns_new_hub_id() -> None:
    """process_decision('approve') returns a non-None new_hub_id when parent hub exists."""
    backend = NullStorageBackend()
    handler = ForkHandler(storage=backend, redis=None)

    # Insert the parent hub so spawn_new_hub can find it
    parent_hub = HubRecord(hub_id="hub-parent", workspace_id="ws-1", initiator="alice")
    await backend.insert_hub(parent_hub)

    event = _make_event()
    record = await handler.handle_detection(event)
    fork_id = record.fork_id

    new_hub_id = await handler.process_decision(fork_id, "approve")

    assert new_hub_id is not None
    assert isinstance(new_hub_id, str)
    assert len(new_hub_id) > 0


@pytest.mark.asyncio
async def test_process_decision_approve_updates_status() -> None:
    """process_decision('approve') sets record status to 'approved' in storage."""
    backend = NullStorageBackend()
    handler = ForkHandler(storage=backend, redis=None)

    parent_hub = HubRecord(hub_id="hub-parent", workspace_id="ws-1", initiator="alice")
    await backend.insert_hub(parent_hub)

    event = _make_event()
    record = await handler.handle_detection(event)
    fork_id = record.fork_id

    await handler.process_decision(fork_id, "approve")

    updated = await backend.get_fork_scenario(fork_id)
    assert updated is not None
    assert updated.status == "approved"
    assert updated.user_decision == "approve"


@pytest.mark.asyncio
async def test_process_decision_reject_returns_none() -> None:
    """process_decision('reject') returns None."""
    backend = NullStorageBackend()
    handler = ForkHandler(storage=backend, redis=None)

    event = _make_event()
    record = await handler.handle_detection(event)

    result = await handler.process_decision(record.fork_id, "reject")

    assert result is None


@pytest.mark.asyncio
async def test_process_decision_reject_updates_status() -> None:
    """process_decision('reject') sets record status to 'rejected' in storage."""
    backend = NullStorageBackend()
    handler = ForkHandler(storage=backend, redis=None)

    event = _make_event()
    record = await handler.handle_detection(event)
    fork_id = record.fork_id

    await handler.process_decision(fork_id, "reject")

    updated = await backend.get_fork_scenario(fork_id)
    assert updated is not None
    assert updated.status == "rejected"
    assert updated.user_decision == "reject"


@pytest.mark.asyncio
async def test_process_decision_invalid_raises_value_error() -> None:
    """process_decision with an unrecognised decision string raises ValueError."""
    backend = NullStorageBackend()
    handler = ForkHandler(storage=backend, redis=None)

    with pytest.raises(ValueError, match="Invalid decision"):
        await handler.process_decision("any-fork-id", "maybe")


# ---------------------------------------------------------------------------
# spawn_new_hub
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_spawn_new_hub_missing_context_returns_none() -> None:
    """spawn_new_hub returns None when no fork context is set."""
    backend = NullStorageBackend()
    handler = ForkHandler(storage=backend, redis=None)

    # No context set for this fork_id
    result = await handler.spawn_new_hub("unknown-fork-id")

    assert result is None


@pytest.mark.asyncio
async def test_spawn_new_hub_missing_parent_hub_returns_none() -> None:
    """spawn_new_hub returns None when the parent hub does not exist in storage."""
    backend = NullStorageBackend()
    handler = ForkHandler(storage=backend, redis=None)

    # Set context but do NOT insert the parent hub
    fork_context.set_context("fork-xyz", {"parent_hub_id": "hub-ghost", "hal_agent_id": "hal-1"})

    result = await handler.spawn_new_hub("fork-xyz")

    assert result is None


@pytest.mark.asyncio
async def test_spawn_new_hub_creates_hub_in_storage() -> None:
    """spawn_new_hub inserts a HubRecord for the new hub into storage."""
    backend = NullStorageBackend()
    handler = ForkHandler(storage=backend, redis=None)

    parent_hub = HubRecord(
        hub_id="hub-parent",
        workspace_id="ws-42",
        initiator="bob",
    )
    await backend.insert_hub(parent_hub)

    fork_context.set_context(
        "fork-abc",
        {"parent_hub_id": "hub-parent", "hal_agent_id": "hal-1"},
    )

    new_hub_id = await handler.spawn_new_hub("fork-abc")

    assert new_hub_id is not None
    created = await backend.get_hub(new_hub_id)
    assert created is not None
    assert created.hub_id == new_hub_id
    assert created.workspace_id == "ws-42"
    assert created.initiator == "bob"
    assert created.state == "active"
    assert created.created_at > 0
