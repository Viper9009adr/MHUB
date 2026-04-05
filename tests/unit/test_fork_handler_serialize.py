"""Unit tests for ForkHandler._serialize_payload and deserialize_payload."""

from __future__ import annotations

import msgpack
import pytest

from src.orc.fork_handler import ForkHandler, FORMAT_MSGPACK, MAX_PAYLOAD_SIZE


class TestSerializePayload:
    """Tests for ForkHandler._serialize_payload (instance method)."""

    @pytest.fixture
    def handler(self) -> ForkHandler:
        from src.storage.null_backend import NullStorageBackend
        return ForkHandler(storage=NullStorageBackend(), redis=None)

    def test_empty_bytes_returns_none(self, handler: ForkHandler) -> None:
        """Empty evidence bytes yields None — nothing to serialize."""
        result = handler._serialize_payload(b"")
        assert result is None

    def test_nonempty_has_format_msgpack_marker(self, handler: ForkHandler) -> None:
        """Non-empty evidence is prefixed with the FORMAT_MSGPACK byte (0x01)."""
        evidence = b"some raw evidence"
        result = handler._serialize_payload(evidence)
        assert result is not None
        assert result[0] == FORMAT_MSGPACK

    def test_serialize_deserialize_round_trip(self, handler: ForkHandler) -> None:
        """Serialized payload can be deserialized back to the original bytes via msgpack."""
        # Pack the bytes into msgpack so deserialize_payload can unpack them
        evidence = msgpack.packb({"key": "value"}, use_bin_type=True)
        serialized = handler._serialize_payload(evidence)
        assert serialized is not None
        result = ForkHandler.deserialize_payload(serialized)
        assert result == {"key": "value"}

    def test_oversized_payload_is_truncated(self, handler: ForkHandler) -> None:
        """Evidence exceeding MAX_PAYLOAD_SIZE is silently truncated before serialization."""
        oversized = b"x" * (MAX_PAYLOAD_SIZE + 1000)
        result = handler._serialize_payload(oversized)
        assert result is not None
        # The body after the 1-byte marker must be exactly MAX_PAYLOAD_SIZE
        assert len(result) == MAX_PAYLOAD_SIZE + 1


class TestDeserializePayload:
    """Tests for ForkHandler.deserialize_payload (static method)."""

    def test_none_returns_none(self) -> None:
        """Passing None to deserialize_payload returns None."""
        result = ForkHandler.deserialize_payload(None)
        assert result is None

    def test_empty_bytes_returns_none(self) -> None:
        """Empty bytes yields None."""
        result = ForkHandler.deserialize_payload(b"")
        assert result is None

    def test_unknown_marker_returns_none(self) -> None:
        """An unrecognised format marker byte yields None."""
        # 0xFF is not a known marker
        result = ForkHandler.deserialize_payload(b"\xff" + b"garbage")
        assert result is None
