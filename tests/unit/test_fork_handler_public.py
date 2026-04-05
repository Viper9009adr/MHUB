"""Unit tests for ForkHandler public API: get_fork_scenario and publish_pending."""

from __future__ import annotations

import pytest

from src.orc import fork_context
from src.orc.fork_handler import DivergenceEvent, ForkHandler
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


@pytest.mark.asyncio
async def test_get_fork_scenario_returns_none_for_unknown() -> None:
    """get_fork_scenario returns None for a fork_id that was never stored."""
    backend = NullStorageBackend()
    handler = ForkHandler(storage=backend, redis=None)

    result = await handler.get_fork_scenario("does-not-exist")

    assert result is None


@pytest.mark.asyncio
async def test_get_fork_scenario_returns_record_after_handle_detection() -> None:
    """get_fork_scenario returns the record that was created by handle_detection."""
    backend = NullStorageBackend()
    handler = ForkHandler(storage=backend, redis=None)
    event = _make_event()

    record = await handler.handle_detection(event)
    retrieved = await handler.get_fork_scenario(record.fork_id)

    assert retrieved is not None
    assert retrieved.fork_id == record.fork_id
    assert retrieved.parent_hub_id == "hub-parent"


@pytest.mark.asyncio
async def test_publish_pending_with_no_redis_is_noop() -> None:
    """publish_pending does not raise when redis=None."""
    from src.storage.models import ForkScenarioRecord

    backend = NullStorageBackend()
    handler = ForkHandler(storage=backend, redis=None)

    dummy_record = ForkScenarioRecord(
        fork_id="fork-noop",
        parent_hub_id="hub-parent",
        divergence_type="state_mismatch",
        divergence_reason="test",
        hal_agent_id="hal-1",
        detected_at=0,
    )

    # Must not raise even though redis is None
    await handler.publish_pending("fork-noop", dummy_record)
