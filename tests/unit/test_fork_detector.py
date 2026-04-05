"""Unit tests for ForkDetector._handle_raw_event and handle_divergence_event."""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock

import msgpack
import pytest

from src.orc import fork_context
from src.orc.fork_detector import ForkDetector
from src.orc.fork_handler import DivergenceEvent


@pytest.fixture(autouse=True)
def clear_fork_contexts() -> None:
    """Clear module-level fork context after every test."""
    yield
    fork_context._contexts.clear()


def _pack_event(**kwargs) -> bytes:
    """Helper: pack a dict as msgpack bytes for use as a raw Redis message."""
    defaults = {
        "parent_hub_id": "hub-parent",
        "hal_agent_id": "hal-007",
        "divergence_type": "state_mismatch",
        "divergence_reason": "counter drifted",
        "evidence": b"",
        "detected_at": 0,
    }
    defaults.update(kwargs)
    return msgpack.packb(defaults, use_bin_type=True)


@pytest.mark.asyncio
async def test_valid_event_dispatches_to_handler() -> None:
    """A valid msgpack event calls ForkHandler.handle_detection exactly once."""
    mock_handler = MagicMock()
    mock_handler.handle_detection = AsyncMock(return_value=MagicMock())
    mock_redis = MagicMock()

    detector = ForkDetector(redis=mock_redis, handler=mock_handler)
    raw = _pack_event()

    await detector._handle_raw_event(raw)

    mock_handler.handle_detection.assert_awaited_once()
    call_arg = mock_handler.handle_detection.call_args[0][0]
    assert isinstance(call_arg, DivergenceEvent)
    assert call_arg.parent_hub_id == "hub-parent"
    assert call_arg.hal_agent_id == "hal-007"


@pytest.mark.asyncio
async def test_invalid_msgpack_logs_error_and_does_not_raise(caplog) -> None:
    """Malformed msgpack bytes are caught; an error is logged; no exception raised."""
    mock_handler = MagicMock()
    mock_handler.handle_detection = AsyncMock()
    mock_redis = MagicMock()

    detector = ForkDetector(redis=mock_redis, handler=mock_handler)

    with caplog.at_level(logging.ERROR, logger="src.orc.fork_detector"):
        await detector._handle_raw_event(b"\xff\xfe invalid msgpack garbage")

    mock_handler.handle_detection.assert_not_awaited()
    assert any("Failed to parse" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_event_missing_parent_hub_id_logs_warning_and_skips(caplog) -> None:
    """Event with empty parent_hub_id is skipped with a warning; handler not called."""
    mock_handler = MagicMock()
    mock_handler.handle_detection = AsyncMock()
    mock_redis = MagicMock()

    detector = ForkDetector(redis=mock_redis, handler=mock_handler)
    raw = _pack_event(parent_hub_id="")  # missing required field

    with caplog.at_level(logging.WARNING, logger="src.orc.fork_detector"):
        await detector._handle_raw_event(raw)

    mock_handler.handle_detection.assert_not_awaited()
    assert any("Incomplete" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_event_missing_hal_agent_id_logs_warning_and_skips(caplog) -> None:
    """Event with empty hal_agent_id is skipped with a warning; handler not called."""
    mock_handler = MagicMock()
    mock_handler.handle_detection = AsyncMock()
    mock_redis = MagicMock()

    detector = ForkDetector(redis=mock_redis, handler=mock_handler)
    raw = _pack_event(hal_agent_id="")  # missing required field

    with caplog.at_level(logging.WARNING, logger="src.orc.fork_detector"):
        await detector._handle_raw_event(raw)

    mock_handler.handle_detection.assert_not_awaited()
    assert any("Incomplete" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_handle_divergence_event_delegates_to_handle_raw_event() -> None:
    """handle_divergence_event is a thin wrapper that calls _handle_raw_event."""
    mock_handler = MagicMock()
    mock_handler.handle_detection = AsyncMock(return_value=MagicMock())
    mock_redis = MagicMock()

    detector = ForkDetector(redis=mock_redis, handler=mock_handler)
    raw = _pack_event()

    await detector.handle_divergence_event(raw)

    # Verify the underlying handler was called (same path as _handle_raw_event)
    mock_handler.handle_detection.assert_awaited_once()
