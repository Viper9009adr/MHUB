"""Unit tests for DivergenceEvent serialization and defaults."""

from __future__ import annotations

import pytest

from src.orc.fork_handler import DivergenceEvent


def _make_event(**kwargs) -> DivergenceEvent:
    defaults = {
        "parent_hub_id": "hub-parent",
        "hal_agent_id": "hal-007",
        "divergence_type": "state_mismatch",
        "divergence_reason": "counter drifted",
    }
    defaults.update(kwargs)
    return DivergenceEvent(**defaults)


class TestDivergenceEventDefaults:
    """Tests that optional fields have correct default values."""

    def test_evidence_default_is_empty_bytes(self) -> None:
        """evidence defaults to b'' when not supplied."""
        event = _make_event()
        assert event.evidence == b""

    def test_detected_at_default_is_zero(self) -> None:
        """detected_at defaults to 0 when not supplied."""
        event = _make_event()
        assert event.detected_at == 0


class TestDivergenceEventDictRoundTrip:
    """Tests for to_dict / from_dict round-trip."""

    def test_to_dict_from_dict_round_trip(self) -> None:
        """to_dict followed by from_dict reconstructs an identical event."""
        original = _make_event(evidence=b"\x01\x02", detected_at=9999)
        reconstructed = DivergenceEvent.from_dict(original.to_dict())
        assert reconstructed.parent_hub_id == original.parent_hub_id
        assert reconstructed.hal_agent_id == original.hal_agent_id
        assert reconstructed.divergence_type == original.divergence_type
        assert reconstructed.divergence_reason == original.divergence_reason
        assert reconstructed.evidence == original.evidence
        assert reconstructed.detected_at == original.detected_at


class TestDivergenceEventMsgpackRoundTrip:
    """Tests for to_msgpack / from_msgpack round-trip."""

    def test_to_msgpack_from_msgpack_round_trip(self) -> None:
        """to_msgpack followed by from_msgpack reconstructs an identical event."""
        original = _make_event(evidence=b"\xde\xad\xbe\xef", detected_at=12345)
        raw = original.to_msgpack()
        assert isinstance(raw, bytes)
        reconstructed = DivergenceEvent.from_msgpack(raw)
        assert reconstructed.parent_hub_id == original.parent_hub_id
        assert reconstructed.hal_agent_id == original.hal_agent_id
        assert reconstructed.divergence_type == original.divergence_type
        assert reconstructed.divergence_reason == original.divergence_reason
        assert reconstructed.evidence == original.evidence
        assert reconstructed.detected_at == original.detected_at
