"""Unit tests for the storage PostgreSQL backend."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.storage.models import HubRecord, CheckpointRecord
from src.storage.pg import MANAGED_SCHEMA_STATEMENTS, StorageBackend, init_storage


@pytest.fixture
def mock_pool() -> MagicMock:
    """Create a mock asyncpg pool."""
    pool = MagicMock()
    pool.acquire = MagicMock()
    pool.close = AsyncMock()
    return pool


@pytest.fixture
def mock_conn() -> AsyncMock:
    """Create a mock asyncpg connection."""
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value="INSERT 0 1")
    conn.fetchrow = AsyncMock(return_value=None)
    return conn


@pytest.fixture
def storage(mock_pool: MagicMock, mock_conn: AsyncMock) -> StorageBackend:
    """Create a StorageBackend with mocked pool."""
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return StorageBackend(mock_pool)


@pytest.mark.asyncio
async def test_init_tables(storage: StorageBackend, mock_pool: MagicMock, mock_conn: AsyncMock) -> None:
    """Test that init_tables executes CREATE TABLE statements."""
    await storage.init_tables()
    assert mock_conn.execute.call_count == len(MANAGED_SCHEMA_STATEMENTS)


@pytest.mark.asyncio
async def test_insert_hub(storage: StorageBackend, mock_conn: AsyncMock) -> None:
    """Test inserting a hub record."""
    record = HubRecord(
        hub_id="h-1",
        workspace_id="ws-1",
        initiator="user-1",
        created_at=1000,
    )
    await storage.insert_hub(record)
    mock_conn.execute.assert_called()
    call_args = mock_conn.execute.call_args
    assert "INSERT INTO hubs" in call_args[0][0]


@pytest.mark.asyncio
async def test_get_hub_found(storage: StorageBackend, mock_conn: AsyncMock) -> None:
    """Test fetching an existing hub."""
    mock_conn.fetchrow.return_value = {
        "hub_id": "h-1",
        "workspace_id": "ws-1",
        "initiator": "user-1",
        "state": "active",
        "created_at": 1000,
        "terminated_reason": None,
    }
    result = await storage.get_hub("h-1")
    assert result is not None
    assert result.hub_id == "h-1"
    assert result.state == "active"


@pytest.mark.asyncio
async def test_get_hub_not_found(storage: StorageBackend, mock_conn: AsyncMock) -> None:
    """Test fetching a non-existent hub returns None."""
    mock_conn.fetchrow.return_value = None
    result = await storage.get_hub("missing")
    assert result is None


@pytest.mark.asyncio
async def test_update_hub_state(storage: StorageBackend, mock_conn: AsyncMock) -> None:
    """Test updating hub state."""
    mock_conn.execute.return_value = "UPDATE 1"
    result = await storage.update_hub_state("h-1", "terminated", "done")
    assert result is True


@pytest.mark.asyncio
async def test_update_hub_state_no_rows(storage: StorageBackend, mock_conn: AsyncMock) -> None:
    """Test updating hub state when no rows match."""
    mock_conn.execute.return_value = "UPDATE 0"
    result = await storage.update_hub_state("h-1", "terminated")
    assert result is False


@pytest.mark.asyncio
async def test_insert_checkpoint(storage: StorageBackend, mock_conn: AsyncMock) -> None:
    """Test inserting a checkpoint record."""
    record = CheckpointRecord(
        checkpoint_id="cp-1",
        hub_id="h-1",
        label="v1",
    )
    await storage.insert_checkpoint(record)
    mock_conn.execute.assert_called()
    call_args = mock_conn.execute.call_args
    assert "INSERT INTO checkpoints" in call_args[0][0]


@pytest.mark.asyncio
async def test_get_checkpoint_found(storage: StorageBackend, mock_conn: AsyncMock) -> None:
    """Test fetching an existing checkpoint."""
    mock_conn.fetchrow.return_value = {
        "checkpoint_id": "cp-1",
        "hub_id": "h-1",
        "label": "v1",
    }
    result = await storage.get_checkpoint("cp-1")
    assert result is not None
    assert result.checkpoint_id == "cp-1"


@pytest.mark.asyncio
async def test_get_checkpoint_not_found(storage: StorageBackend, mock_conn: AsyncMock) -> None:
    """Test fetching a non-existent checkpoint returns None."""
    mock_conn.fetchrow.return_value = None
    result = await storage.get_checkpoint("missing")
    assert result is None


@pytest.mark.asyncio
async def test_close(storage: StorageBackend, mock_pool: MagicMock) -> None:
    """Test closing the storage backend."""
    await storage.close()
    mock_pool.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_init_storage() -> None:
    """Test init_storage creates pool and initialises tables."""
    mock_pool = MagicMock()
    mock_pool.close = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)

    with patch("src.storage.pg.asyncpg.create_pool", new_callable=AsyncMock, return_value=mock_pool):
        backend = await init_storage("postgresql://test", pool_size=3)
        assert isinstance(backend, StorageBackend)
        assert mock_conn.execute.call_count == len(MANAGED_SCHEMA_STATEMENTS)
