"""Unit tests for the ORC daemon."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.orc.daemon import OrcDaemon, orc_daemon_lifespan
from src.storage.models import ForkScenarioRecord


def test_default_grpc_port_is_not_50051_no_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MERIDIAN_GRPC_PORT", raising=False)
    daemon = OrcDaemon()
    assert daemon.grpc_port != 50051


@pytest.fixture
def mock_storage() -> AsyncMock:
    """Create a mock storage backend."""
    storage = AsyncMock()
    storage.init_tables = AsyncMock()
    storage.close = AsyncMock()
    return storage


@pytest.fixture
def mock_grpc_server() -> AsyncMock:
    """Create a mock gRPC server."""
    server = AsyncMock()
    server.start = AsyncMock()
    server.stop = AsyncMock()
    server.add_insecure_port = MagicMock(return_value=50051)
    server.add_generic_rpc_handlers = MagicMock()
    server.add_registered_method_handlers = MagicMock()
    return server


@pytest.mark.asyncio
async def test_daemon_start(
    mock_storage: AsyncMock, mock_grpc_server: AsyncMock
) -> None:
    """Test daemon starts gRPC server and storage."""
    with (
        patch("src.orc.daemon.init_storage", new_callable=AsyncMock, return_value=mock_storage),
        patch(
            "src.orc.daemon.grpc.aio.server",
            side_effect=lambda *args, **kwargs: mock_grpc_server,
        ),
    ):
        daemon = OrcDaemon(grpc_port=50052, database_url="postgresql://test", db_pool_size=3)
        await daemon.start()

        assert daemon.hub_service is not None
        assert daemon.storage is not None
        assert daemon.grpc_port == 50052
        mock_grpc_server.start.assert_awaited_once()
        mock_grpc_server.add_insecure_port.assert_called_once_with("[::]:50052")


@pytest.mark.asyncio
async def test_daemon_stop(
    mock_storage: AsyncMock, mock_grpc_server: AsyncMock
) -> None:
    """Test daemon stops gRPC server and storage."""
    with (
        patch("src.orc.daemon.init_storage", new_callable=AsyncMock, return_value=mock_storage),
        patch(
            "src.orc.daemon.grpc.aio.server",
            side_effect=lambda *args, **kwargs: mock_grpc_server,
        ),
    ):
        daemon = OrcDaemon()
        await daemon.start()
        await daemon.stop()

        mock_grpc_server.stop.assert_awaited_once_with(grace=5)
        mock_storage.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_daemon_lifespan(
    mock_storage: AsyncMock, mock_grpc_server: AsyncMock
) -> None:
    """Test lifespan context manager starts and stops daemon."""
    with (
        patch("src.orc.daemon.init_storage", new_callable=AsyncMock, return_value=mock_storage),
        patch(
            "src.orc.daemon.grpc.aio.server",
            side_effect=lambda *args, **kwargs: mock_grpc_server,
        ),
    ):
        async with orc_daemon_lifespan(grpc_port=50053) as daemon:
            assert isinstance(daemon, OrcDaemon)
            assert daemon.hub_service is not None

        mock_grpc_server.stop.assert_awaited_once()
        mock_storage.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_daemon_lifespan_on_exception(
    mock_storage: AsyncMock, mock_grpc_server: AsyncMock
) -> None:
    """Test lifespan context manager stops daemon even on exception."""
    with (
        patch("src.orc.daemon.init_storage", new_callable=AsyncMock, return_value=mock_storage),
        patch(
            "src.orc.daemon.grpc.aio.server",
            side_effect=lambda *args, **kwargs: mock_grpc_server,
        ),
    ):
        with pytest.raises(RuntimeError):
            async with orc_daemon_lifespan() as daemon:
                raise RuntimeError("test error")

        mock_grpc_server.stop.assert_awaited_once()
        mock_storage.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_daemon_start_recovers_pending_forks_without_runtime_warning(
    mock_storage: AsyncMock,
    mock_grpc_server: AsyncMock,
    recwarn: pytest.WarningsRecorder,
) -> None:
    """Redis-enabled startup recovers pending forks and exits warning-clean."""
    pending = ForkScenarioRecord(
        fork_id="fork-1",
        parent_hub_id="hub-parent",
        divergence_type="state_mismatch",
        divergence_reason="counter drifted",
        hal_agent_id="hal-007",
        detected_at=1,
        status="pending",
        divergence_payload=b"\x01proof",
    )
    mock_storage.get_pending_forks = AsyncMock(return_value=[pending])

    mock_redis = AsyncMock()
    mock_redis.connect = AsyncMock()
    mock_redis.disconnect = AsyncMock()
    mock_redis.publish = AsyncMock()

    listener_task: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(3600))

    async def _stop_listener() -> None:
        listener_task.cancel()
        with suppress(asyncio.CancelledError):
            await listener_task

    mock_detector = MagicMock()
    mock_detector.listen_for_divergence = MagicMock(return_value=listener_task)
    mock_detector.stop = AsyncMock(side_effect=_stop_listener)

    with (
        patch("src.orc.daemon.init_storage", new_callable=AsyncMock, return_value=mock_storage),
        patch(
            "src.orc.daemon.grpc.aio.server",
            side_effect=lambda *args, **kwargs: mock_grpc_server,
        ),
        patch("src.orc.daemon.RedisBridge", return_value=mock_redis),
        patch("src.orc.daemon.ForkDetector", return_value=mock_detector),
    ):
        daemon = OrcDaemon(redis_enabled=True)
        await daemon.start()
        await daemon.stop()

    mock_storage.get_pending_forks.assert_awaited_once()
    mock_redis.publish.assert_awaited_once()
    mock_detector.listen_for_divergence.assert_called_once()
    mock_detector.stop.assert_awaited_once()
    runtime_warnings = [w for w in recwarn if issubclass(w.category, RuntimeWarning)]
    assert runtime_warnings == []
