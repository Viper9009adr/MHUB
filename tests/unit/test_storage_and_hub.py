"""Unit tests for storage backends, hub service, and circuit breaker."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.storage.null_backend import NullStorageBackend
from src.storage.retry import (
    RetryConfig,
    with_retry,
    _calculate_delay,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    CircuitState,
)
from src.storage.models import HubRecord, CheckpointRecord
from src.hub.service import HubService


# ---------------------------------------------------------------------------
# NullStorageBackend tests
# ---------------------------------------------------------------------------


class TestNullStorageBackend:
    """Tests for the in-memory null storage backend."""

    @pytest.fixture
    def backend(self) -> NullStorageBackend:
        return NullStorageBackend()

    @pytest.mark.asyncio
    async def test_init_tables_is_noop(self, backend: NullStorageBackend) -> None:
        await backend.init_tables()  # should not raise

    @pytest.mark.asyncio
    async def test_insert_and_get_hub(self, backend: NullStorageBackend) -> None:
        record = HubRecord(
            hub_id="h1",
            workspace_id="w1",
            initiator="alice",
        )
        await backend.insert_hub(record)
        result = await backend.get_hub("h1")
        assert result is not None
        assert result.hub_id == "h1"
        assert result.workspace_id == "w1"
        assert result.initiator == "alice"

    @pytest.mark.asyncio
    async def test_get_missing_hub_returns_none(self, backend: NullStorageBackend) -> None:
        result = await backend.get_hub("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_update_hub_state(self, backend: NullStorageBackend) -> None:
        record = HubRecord(hub_id="h1", workspace_id="w1", initiator="alice")
        await backend.insert_hub(record)
        updated = await backend.update_hub_state("h1", "terminated", "done")
        assert updated is True
        hub = await backend.get_hub("h1")
        assert hub is not None
        assert hub.state == "terminated"
        assert hub.terminated_reason == "done"

    @pytest.mark.asyncio
    async def test_update_missing_hub_returns_false(self, backend: NullStorageBackend) -> None:
        result = await backend.update_hub_state("nope", "active")
        assert result is False

    @pytest.mark.asyncio
    async def test_insert_and_get_checkpoint(self, backend: NullStorageBackend) -> None:
        cp = CheckpointRecord(
            checkpoint_id="cp1",
            hub_id="h1",
            label="v1",
        )
        await backend.insert_checkpoint(cp)
        result = await backend.get_checkpoint("cp1")
        assert result is not None
        assert result.checkpoint_id == "cp1"
        assert result.label == "v1"

    @pytest.mark.asyncio
    async def test_get_missing_checkpoint_returns_none(self, backend: NullStorageBackend) -> None:
        result = await backend.get_checkpoint("nope")
        assert result is None

    @pytest.mark.asyncio
    async def test_close_is_noop(self, backend: NullStorageBackend) -> None:
        await backend.close()  # should not raise

    @pytest.mark.asyncio
    async def test_list_hubs(self, backend: NullStorageBackend) -> None:
        r1 = HubRecord(hub_id="h1", workspace_id="w1", initiator="a")
        r2 = HubRecord(hub_id="h2", workspace_id="w2", initiator="b")
        await backend.insert_hub(r1)
        await backend.insert_hub(r2)
        hubs = await backend.list_hubs()
        assert len(hubs) == 2
        hub_ids = {h.hub_id for h in hubs}
        assert hub_ids == {"h1", "h2"}

    @pytest.mark.asyncio
    async def test_list_checkpoints(self, backend: NullStorageBackend) -> None:
        cp1 = CheckpointRecord(checkpoint_id="cp1", hub_id="h1", label="v1")
        cp2 = CheckpointRecord(checkpoint_id="cp2", hub_id="h1", label="v2")
        await backend.insert_checkpoint(cp1)
        await backend.insert_checkpoint(cp2)
        cps = await backend.list_checkpoints()
        assert len(cps) == 2


# ---------------------------------------------------------------------------
# Retry tests
# ---------------------------------------------------------------------------


class TestRetry:
    """Tests for the retry decorator and configuration."""

    @pytest.mark.asyncio
    async def test_successful_call_no_retry(self) -> None:
        call_count = 0

        @with_retry(RetryConfig(max_attempts=3))
        async def succeed() -> str:
            nonlocal call_count
            call_count += 1
            return "ok"

        result = await succeed()
        assert result == "ok"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_retry_on_failure_then_succeed(self) -> None:
        call_count = 0

        @with_retry(
            RetryConfig(
                max_attempts=3,
                base_delay=0.001,
                max_delay=0.001,
                jitter=False,
                retryable_exceptions=(ValueError,),
            )
        )
        async def flaky() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("transient")
            return "ok"

        result = await flaky()
        assert result == "ok"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_all_attempts_exhausted(self) -> None:
        call_count = 0

        @with_retry(
            RetryConfig(
                max_attempts=2,
                base_delay=0.001,
                max_delay=0.001,
                jitter=False,
                retryable_exceptions=(ValueError,),
            )
        )
        async def always_fail() -> str:
            nonlocal call_count
            call_count += 1
            raise ValueError("permanent")

        with pytest.raises(ValueError, match="permanent"):
            await always_fail()
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_non_retryable_exception_propagates(self) -> None:
        call_count = 0

        @with_retry(
            RetryConfig(
                max_attempts=3,
                base_delay=0.001,
                retryable_exceptions=(ValueError,),
            )
        )
        async def raise_type_error() -> str:
            nonlocal call_count
            call_count += 1
            raise TypeError("not retryable")

        with pytest.raises(TypeError, match="not retryable"):
            await raise_type_error()
        assert call_count == 1

    def test_retry_config_defaults(self) -> None:
        config = RetryConfig()
        assert config.max_attempts == 3
        assert config.base_delay == 0.1
        assert config.max_delay == 5.0
        assert config.exponential_base == 2.0
        assert config.jitter is True

    def test_calculate_delay_respects_max(self) -> None:
        config = RetryConfig(
            base_delay=1.0,
            max_delay=0.5,
            exponential_base=2.0,
            jitter=False,
        )
        # 1.0 * 2^3 = 8.0, capped at 0.5
        delay = _calculate_delay(3, config)
        assert delay == 0.5


# ---------------------------------------------------------------------------
# CircuitBreaker tests
# ---------------------------------------------------------------------------


class TestCircuitBreaker:
    """Tests for the circuit breaker (closed / open / half-open)."""

    @pytest.fixture
    def breaker(self) -> CircuitBreaker:
        return CircuitBreaker(
            CircuitBreakerConfig(
                failure_threshold=3,
                cooldown_seconds=0.1,
                half_open_max_calls=1,
            )
        )

    def test_initial_state_is_closed(self, breaker: CircuitBreaker) -> None:
        assert breaker.state is CircuitState.CLOSED
        assert breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_stays_closed_under_success(self, breaker: CircuitBreaker) -> None:
        @breaker
        async def ok() -> str:
            return "yes"

        for _ in range(10):
            assert await ok() == "yes"
        assert breaker.state is CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_opens_after_threshold(self, breaker: CircuitBreaker) -> None:
        call_count = 0

        @breaker
        async def fail() -> str:
            nonlocal call_count
            call_count += 1
            raise ConnectionError("db down")

        for _ in range(3):
            with pytest.raises(ConnectionError):
                await fail()

        assert breaker.state is CircuitState.OPEN
        assert breaker.failure_count == 3

    @pytest.mark.asyncio
    async def test_open_circuit_raises_immediately(self, breaker: CircuitBreaker) -> None:
        @breaker
        async def fail() -> str:
            raise ConnectionError("db down")

        # Trip the breaker
        for _ in range(3):
            with pytest.raises(ConnectionError):
                await fail()

        # Next call should be rejected without touching the function
        with pytest.raises(CircuitOpenError):
            await fail()

    @pytest.mark.asyncio
    async def test_half_open_after_cooldown(self, breaker: CircuitBreaker) -> None:
        @breaker
        async def fail() -> str:
            raise ConnectionError("down")

        # Trip the breaker
        for _ in range(3):
            with pytest.raises(ConnectionError):
                await fail()
        assert breaker.state is CircuitState.OPEN

        # Wait for cooldown
        await asyncio.sleep(0.15)
        assert breaker.state is CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_half_open_success_closes(self, breaker: CircuitBreaker) -> None:
        call_count = 0

        @breaker
        async def sometimes_fail() -> str:
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                raise ConnectionError("down")
            return "recovered"

        # Trip the breaker
        for _ in range(3):
            with pytest.raises(ConnectionError):
                await sometimes_fail()

        # Wait for half-open
        await asyncio.sleep(0.15)
        assert breaker.state is CircuitState.HALF_OPEN

        # Probe succeeds
        result = await sometimes_fail()
        assert result == "recovered"
        assert breaker.state is CircuitState.CLOSED
        assert breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_half_open_failure_reopens(self, breaker: CircuitBreaker) -> None:
        @breaker
        async def always_fail() -> str:
            raise ConnectionError("still down")

        # Trip the breaker
        for _ in range(3):
            with pytest.raises(ConnectionError):
                await always_fail()

        # Wait for half-open
        await asyncio.sleep(0.15)
        assert breaker.state is CircuitState.HALF_OPEN

        # Probe fails — should re-open
        with pytest.raises(ConnectionError):
            await always_fail()
        assert breaker.state is CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_half_open_max_probe_calls(self, breaker: CircuitBreaker) -> None:
        @breaker
        async def always_fail() -> str:
            raise ConnectionError("down")

        # Trip the breaker
        for _ in range(3):
            with pytest.raises(ConnectionError):
                await always_fail()

        # Wait for half-open
        await asyncio.sleep(0.15)

        # First probe call raises ConnectionError (allowed)
        with pytest.raises(ConnectionError):
            await always_fail()

        # Now the breaker is re-opened; next call raises CircuitOpenError
        with pytest.raises(CircuitOpenError):
            await always_fail()

    def test_reset(self, breaker: CircuitBreaker) -> None:
        # Manually force open
        breaker._state = CircuitState.OPEN
        breaker._failure_count = 99
        breaker.reset()
        assert breaker.state is CircuitState.CLOSED
        assert breaker.failure_count == 0

    def test_default_config(self) -> None:
        cb = CircuitBreaker()
        assert cb._config.failure_threshold == 5
        assert cb._config.cooldown_seconds == 30.0
        assert cb._config.half_open_max_calls == 1


# ---------------------------------------------------------------------------
# HubService tests (in-memory / no storage)
# ---------------------------------------------------------------------------


class TestHubService:
    """Tests for the gRPC HubService servicer (no storage backend)."""

    @pytest.fixture
    def service(self) -> HubService:
        return HubService()

    @pytest.fixture
    def mock_context(self) -> MagicMock:
        return MagicMock()

    def test_create_hub(self, service: HubService, mock_context: MagicMock) -> None:
        from src.hub.hub_pb2 import CreateHubRequest

        req = CreateHubRequest(workspace_id="w1", initiator="alice")
        resp = service.CreateHub(req, mock_context)
        assert resp.hub_id != ""
        assert resp.created_at > 0

    def test_terminate_hub(self, service: HubService, mock_context: MagicMock) -> None:
        from src.hub.hub_pb2 import CreateHubRequest, TerminateHubRequest

        create_req = CreateHubRequest(workspace_id="w1", initiator="alice")
        create_resp = service.CreateHub(create_req, mock_context)
        term_req = TerminateHubRequest(hub_id=create_resp.hub_id, reason="done")
        resp = service.TerminateHub(term_req, mock_context)
        assert resp.ok is True

    def test_terminate_missing_hub(self, service: HubService, mock_context: MagicMock) -> None:
        from src.hub.hub_pb2 import TerminateHubRequest

        req = TerminateHubRequest(hub_id="nonexistent", reason="gone")
        resp = service.TerminateHub(req, mock_context)
        assert resp.ok is False
        mock_context.set_code.assert_called_once()

    def test_create_checkpoint(self, service: HubService, mock_context: MagicMock) -> None:
        from src.hub.hub_pb2 import CreateHubRequest, CreateCheckpointRequest

        create_req = CreateHubRequest(workspace_id="w1", initiator="alice")
        create_resp = service.CreateHub(create_req, mock_context)
        cp_req = CreateCheckpointRequest(
            hub_id=create_resp.hub_id, label="v1"
        )
        resp = service.CreateCheckpoint(cp_req, mock_context)
        assert resp.checkpoint_id != ""

    def test_create_checkpoint_missing_hub(
        self, service: HubService, mock_context: MagicMock
    ) -> None:
        from src.hub.hub_pb2 import CreateCheckpointRequest

        req = CreateCheckpointRequest(hub_id="nope", label="v1")
        resp = service.CreateCheckpoint(req, mock_context)
        assert resp.checkpoint_id == ""
        mock_context.set_code.assert_called_once()

    def test_hub_status(self, service: HubService, mock_context: MagicMock) -> None:
        from src.hub.hub_pb2 import CreateHubRequest, HubStatusRequest

        create_req = CreateHubRequest(workspace_id="w1", initiator="alice")
        create_resp = service.CreateHub(create_req, mock_context)
        status_req = HubStatusRequest(hub_id=create_resp.hub_id)
        resp = service.HubStatus(status_req, mock_context)
        assert resp.hub_id == create_resp.hub_id
        assert resp.state == "active"

    def test_hub_status_missing(
        self, service: HubService, mock_context: MagicMock
    ) -> None:
        from src.hub.hub_pb2 import HubStatusRequest

        req = HubStatusRequest(hub_id="nope")
        resp = service.HubStatus(req, mock_context)
        assert resp.state == "unknown"


# ---------------------------------------------------------------------------
# HubService tests (with NullStorageBackend)
# ---------------------------------------------------------------------------


class TestHubServiceWithStorage:
    """Tests for HubService backed by NullStorageBackend."""

    @pytest.fixture
    def backend(self) -> NullStorageBackend:
        return NullStorageBackend()

    @pytest.fixture
    def service(self, backend: NullStorageBackend) -> HubService:
        return HubService(storage=backend)

    @pytest.fixture
    def mock_context(self) -> MagicMock:
        return MagicMock()

    def test_service_has_storage(self, service: HubService) -> None:
        assert service._has_storage() is True

    def test_service_without_storage(self) -> None:
        svc = HubService()
        assert svc._has_storage() is False

    def test_create_hub_with_storage(
        self, service: HubService, mock_context: MagicMock
    ) -> None:
        from src.hub.hub_pb2 import CreateHubRequest

        req = CreateHubRequest(workspace_id="w1", initiator="alice")
        resp = service.CreateHub(req, mock_context)
        assert resp.hub_id != ""
        assert resp.created_at > 0

    @pytest.mark.asyncio
    async def test_create_hub_persists_to_storage(
        self, backend: NullStorageBackend, mock_context: MagicMock
    ) -> None:
        from src.hub.hub_pb2 import CreateHubRequest

        service = HubService(storage=backend)
        req = CreateHubRequest(workspace_id="w1", initiator="alice")

        # Run in a context without a running event loop so the sync
        # fallback path in CreateHub uses run_until_complete.
        resp = service.CreateHub(req, mock_context)
        hub_id = resp.hub_id

        # Verify the record exists in storage
        record = await backend.get_hub(hub_id)
        assert record is not None
        assert record.workspace_id == "w1"
        assert record.initiator == "alice"
        assert record.state == "active"

    @pytest.mark.asyncio
    async def test_terminate_hub_with_storage(
        self, backend: NullStorageBackend, mock_context: MagicMock
    ) -> None:
        from src.hub.hub_pb2 import CreateHubRequest, TerminateHubRequest

        service = HubService(storage=backend)
        create_req = CreateHubRequest(workspace_id="w1", initiator="alice")
        create_resp = service.CreateHub(create_req, mock_context)

        term_req = TerminateHubRequest(hub_id=create_resp.hub_id, reason="done")
        resp = service.TerminateHub(term_req, mock_context)
        assert resp.ok is True

    @pytest.mark.asyncio
    async def test_checkpoint_with_storage(
        self, backend: NullStorageBackend, mock_context: MagicMock
    ) -> None:
        from src.hub.hub_pb2 import CreateHubRequest, CreateCheckpointRequest

        service = HubService(storage=backend)
        create_req = CreateHubRequest(workspace_id="w1", initiator="alice")
        create_resp = service.CreateHub(create_req, mock_context)

        cp_req = CreateCheckpointRequest(
            hub_id=create_resp.hub_id, label="v1"
        )
        resp = service.CreateCheckpoint(cp_req, mock_context)
        assert resp.checkpoint_id != ""

    @pytest.mark.asyncio
    async def test_hub_status_with_storage(
        self, backend: NullStorageBackend, mock_context: MagicMock
    ) -> None:
        from src.hub.hub_pb2 import CreateHubRequest, HubStatusRequest

        service = HubService(storage=backend)
        create_req = CreateHubRequest(workspace_id="w1", initiator="alice")
        create_resp = service.CreateHub(create_req, mock_context)

        status_req = HubStatusRequest(hub_id=create_resp.hub_id)
        resp = service.HubStatus(status_req, mock_context)
        assert resp.hub_id == create_resp.hub_id
        assert resp.state == "active"
