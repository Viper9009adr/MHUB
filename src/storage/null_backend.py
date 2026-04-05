"""Null (in-memory) storage backend for testing and development.

Provides a no-op implementation of the storage interface that keeps
data in memory without persisting to any database. Useful for unit
tests and local development where a real PostgreSQL instance is
unavailable.
"""

from __future__ import annotations

from typing import Any

from src.storage.models import HubRecord, CheckpointRecord, ForkScenarioRecord


class NullStorageBackend:
    """In-memory storage backend that mimics the asyncpg interface.

    All data is kept in plain Python dicts and is lost when the
    instance is garbage-collected. No actual I/O occurs.
    """

    def __init__(self) -> None:
        self._hubs: dict[str, HubRecord] = {}
        self._checkpoints: dict[str, CheckpointRecord] = {}
        self._fork_scenarios: dict[str, ForkScenarioRecord] = {}

    async def init_tables(self) -> None:
        """No-op: tables do not exist in the null backend."""

    async def insert_hub(self, record: HubRecord) -> None:
        """Store a hub record in memory."""
        self._hubs[record.hub_id] = record

    async def get_hub(self, hub_id: str) -> HubRecord | None:
        """Fetch a hub record by ID from memory."""
        return self._hubs.get(hub_id)

    async def update_hub_state(
        self, hub_id: str, state: str, reason: str | None = None
    ) -> bool:
        """Update the state of a hub in memory. Returns True if found."""
        hub = self._hubs.get(hub_id)
        if hub is None:
            return False
        hub.state = state
        hub.terminated_reason = reason
        return True

    async def insert_checkpoint(self, record: CheckpointRecord) -> None:
        """Store a checkpoint record in memory."""
        self._checkpoints[record.checkpoint_id] = record

    async def get_checkpoint(
        self, checkpoint_id: str
    ) -> CheckpointRecord | None:
        """Fetch a checkpoint record by ID from memory."""
        return self._checkpoints.get(checkpoint_id)

    async def close(self) -> None:
        """No-op: nothing to close in the null backend."""

    async def list_hubs(self) -> list[HubRecord]:
        """Return all hub records currently in memory."""
        return list(self._hubs.values())

    async def list_checkpoints(self) -> list[CheckpointRecord]:
        """Return all checkpoint records currently in memory."""
        return list(self._checkpoints.values())

    async def get_pending_forks(self) -> list[ForkScenarioRecord]:
        """Return all pending fork scenarios currently in memory."""
        return [
            r for r in self._fork_scenarios.values() if r.status == "pending"
        ]

    async def insert_fork_scenario(self, record: ForkScenarioRecord) -> None:
        """Store a fork scenario record in memory."""
        self._fork_scenarios[record.fork_id] = record

    async def get_fork_scenario(
        self, fork_id: str
    ) -> ForkScenarioRecord | None:
        """Fetch a fork scenario record by ID from memory."""
        return self._fork_scenarios.get(fork_id)

    async def update_fork_decision(
        self,
        fork_id: str,
        decision: str,
        new_hub_id: str | None,
        status: str,
        decided_at: int,
    ) -> bool:
        """Update the decision on a fork scenario in memory. Returns True if found."""
        record = self._fork_scenarios.get(fork_id)
        if record is None:
            return False
        record.user_decision = decision
        record.new_hub_id = new_hub_id
        record.status = status
        record.decided_at = decided_at
        return True
