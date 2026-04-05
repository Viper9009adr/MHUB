"""PostgreSQL storage backend for Meridian HUB.

Provides programmatic CREATE TABLE IF NOT EXISTS on startup
and basic CRUD operations for hubs and checkpoints.

Connection failures are automatically retried using the
configurable retry decorator from src.storage.retry.
"""

from __future__ import annotations

import json

from typing import Any

import asyncpg

from src.storage.models import (
    AgentSessionRecord,
    ApprovalRequestRecord,
    AuditLogRecord,
    CheckpointRecord,
    ForkScenarioRecord,
    HubRecord,
)
from src.storage.retry import RetryConfig, with_retry

CREATE_HUBS_TABLE = """
CREATE TABLE IF NOT EXISTS hubs (
    hub_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    initiator TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'active',
    created_at BIGINT NOT NULL DEFAULT 0,
    terminated_reason TEXT
);
"""

CREATE_CHECKPOINTS_TABLE = """
CREATE TABLE IF NOT EXISTS checkpoints (
    checkpoint_id TEXT PRIMARY KEY,
    hub_id TEXT NOT NULL REFERENCES hubs(hub_id),
    label TEXT NOT NULL
);
"""

CREATE_FORK_SCENARIOS_TABLE = """
CREATE TABLE IF NOT EXISTS fork_scenarios (
    fork_id TEXT PRIMARY KEY,
    parent_hub_id TEXT NOT NULL REFERENCES hubs(hub_id),
    divergence_type TEXT NOT NULL,
    divergence_reason TEXT NOT NULL,
    hal_agent_id TEXT NOT NULL,
    detected_at BIGINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    user_decision TEXT,
    decided_at BIGINT,
    new_hub_id TEXT,
    divergence_payload BYTEA,
    CHECK (status IN ('pending', 'approved', 'rejected', 'resolved', 'failed')),
    CHECK (user_decision IS NULL OR user_decision IN ('approve', 'reject'))
);
CREATE INDEX IF NOT EXISTS idx_fork_scenarios_parent ON fork_scenarios(parent_hub_id);
"""

CREATE_AGENT_SESSIONS_TABLE = """
CREATE TABLE IF NOT EXISTS agent_sessions (
    session_id TEXT PRIMARY KEY,
    hub_id TEXT NOT NULL REFERENCES hubs(hub_id),
    agent_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    started_at BIGINT NOT NULL DEFAULT 0,
    ended_at BIGINT,
    CHECK (status IN ('active', 'completed', 'failed', 'cancelled'))
);
CREATE INDEX IF NOT EXISTS idx_agent_sessions_hub ON agent_sessions(hub_id);
CREATE INDEX IF NOT EXISTS idx_agent_sessions_agent ON agent_sessions(agent_id);
"""

CREATE_APPROVAL_REQUESTS_TABLE = """
CREATE TABLE IF NOT EXISTS approval_requests (
    request_id TEXT PRIMARY KEY,
    fork_id TEXT NOT NULL REFERENCES fork_scenarios(fork_id),
    status TEXT NOT NULL DEFAULT 'pending',
    created_at BIGINT NOT NULL DEFAULT 0,
    decided_at BIGINT,
    decision TEXT,
    CHECK (status IN ('pending', 'approved', 'rejected', 'expired')),
    CHECK (decision IS NULL OR decision IN ('approve', 'reject'))
);
CREATE INDEX IF NOT EXISTS idx_approval_requests_fork ON approval_requests(fork_id);
"""

CREATE_AUDIT_LOGS_TABLE = """
CREATE TABLE IF NOT EXISTS audit_logs (
    log_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    action TEXT NOT NULL,
    resource TEXT NOT NULL,
    timestamp BIGINT NOT NULL DEFAULT 0,
    metadata JSONB
);
CREATE INDEX IF NOT EXISTS idx_audit_logs_agent ON audit_logs(agent_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_timestamp ON audit_logs(timestamp);
"""

MANAGED_SCHEMA_STATEMENTS = (
    CREATE_HUBS_TABLE,
    CREATE_CHECKPOINTS_TABLE,
    CREATE_FORK_SCENARIOS_TABLE,
    CREATE_AGENT_SESSIONS_TABLE,
    CREATE_APPROVAL_REQUESTS_TABLE,
    CREATE_AUDIT_LOGS_TABLE,
)


DEFAULT_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=0.1,
    max_delay=5.0,
    retryable_exceptions=(
        ConnectionError,
        OSError,
        asyncpg.PostgresError,
    ),
)


class StorageBackend:
    """Async PostgreSQL storage backend using asyncpg.

    All database operations are wrapped with automatic retry
    using exponential back-off and jitter.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        retry_config: RetryConfig | None = None,
    ) -> None:
        self._pool = pool
        self._retry_config = retry_config or DEFAULT_RETRY_CONFIG

    async def init_tables(self) -> None:
        """Run CREATE TABLE IF NOT EXISTS for all managed tables."""
        async with self._pool.acquire() as conn:
            for statement in MANAGED_SCHEMA_STATEMENTS:
                await conn.execute(statement)

    @with_retry()
    async def insert_hub(self, record: HubRecord) -> None:
        """Insert a hub record into the database."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO hubs (hub_id, workspace_id, initiator, state, created_at, terminated_reason)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (hub_id) DO NOTHING
                """,
                record.hub_id,
                record.workspace_id,
                record.initiator,
                record.state,
                record.created_at,
                record.terminated_reason,
            )

    @with_retry()
    async def get_hub(self, hub_id: str) -> HubRecord | None:
        """Fetch a hub record by ID."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM hubs WHERE hub_id = $1", hub_id
            )
            if row is None:
                return None
            return HubRecord.from_dict(dict(row))

    @with_retry()
    async def update_hub_state(
        self,
        hub_id: str,
        state: str,
        reason: str | None = None,
    ) -> bool:
        """Update the state of a hub. Returns True if a row was updated."""
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE hubs SET state = $2, terminated_reason = $3
                WHERE hub_id = $1
                """,
                hub_id,
                state,
                reason,
            )
            return result.split()[-1] != "0"

    @with_retry()
    async def insert_checkpoint(self, record: CheckpointRecord) -> None:
        """Insert a checkpoint record into the database."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO checkpoints (checkpoint_id, hub_id, label)
                VALUES ($1, $2, $3)
                ON CONFLICT (checkpoint_id) DO NOTHING
                """,
                record.checkpoint_id,
                record.hub_id,
                record.label,
            )

    @with_retry()
    async def get_checkpoint(self, checkpoint_id: str) -> CheckpointRecord | None:
        """Fetch a checkpoint record by ID."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM checkpoints WHERE checkpoint_id = $1", checkpoint_id
            )
            if row is None:
                return None
            return CheckpointRecord.from_dict(dict(row))

    async def close(self) -> None:
        """Close the connection pool."""
        await self._pool.close()

    # -- Fork scenario CRUD ------------------------------------------------

    @with_retry()
    async def insert_fork_scenario(self, record: ForkScenarioRecord) -> None:
        """Insert a fork scenario record into the database."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO fork_scenarios (
                    fork_id, parent_hub_id, divergence_type, divergence_reason,
                    hal_agent_id, detected_at, status, user_decision,
                    decided_at, new_hub_id, divergence_payload
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (fork_id) DO NOTHING
                """,
                record.fork_id,
                record.parent_hub_id,
                record.divergence_type,
                record.divergence_reason,
                record.hal_agent_id,
                record.detected_at,
                record.status,
                record.user_decision,
                record.decided_at,
                record.new_hub_id,
                record.divergence_payload,
            )

    @with_retry()
    async def get_fork_scenario(self, fork_id: str) -> ForkScenarioRecord | None:
        """Fetch a fork scenario record by ID."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM fork_scenarios WHERE fork_id = $1", fork_id
            )
            if row is None:
                return None
            return ForkScenarioRecord.from_dict(dict(row))

    @with_retry()
    async def update_fork_decision(
        self,
        fork_id: str,
        decision: str,
        new_hub_id: str | None,
        status: str,
        decided_at: int,
    ) -> bool:
        """Update the decision for a fork scenario. Returns True if updated."""
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE fork_scenarios
                SET user_decision = $2, new_hub_id = $3, status = $4, decided_at = $5
                WHERE fork_id = $1
                """,
                fork_id,
                decision,
                new_hub_id,
                status,
                decided_at,
            )
            return result.split()[-1] != "0"

    @with_retry()
    async def get_pending_forks(self) -> list[ForkScenarioRecord]:
        """Fetch all fork scenarios with status 'pending'."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM fork_scenarios WHERE status = $1", "pending"
            )
            return [ForkScenarioRecord.from_dict(dict(row)) for row in rows]

    # -- Agent session CRUD -----------------------------------------------

    @with_retry()
    async def insert_agent_session(self, record: AgentSessionRecord) -> None:
        """Insert an agent session record into the database."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO agent_sessions (
                    session_id, hub_id, agent_id, run_id, status, started_at, ended_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (session_id) DO NOTHING
                """,
                record.session_id,
                record.hub_id,
                record.agent_id,
                record.run_id,
                record.status,
                record.started_at,
                record.ended_at,
            )

    @with_retry()
    async def get_agent_session(self, session_id: str) -> AgentSessionRecord | None:
        """Fetch an agent session record by ID."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM agent_sessions WHERE session_id = $1", session_id
            )
            if row is None:
                return None
            return AgentSessionRecord.from_dict(dict(row))

    @with_retry()
    async def update_agent_session_status(
        self,
        session_id: str,
        status: str,
        ended_at: int | None = None,
    ) -> bool:
        """Update the status of an agent session. Returns True if updated."""
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE agent_sessions SET status = $2, ended_at = $3
                WHERE session_id = $1
                """,
                session_id,
                status,
                ended_at,
            )
            return result.split()[-1] != "0"

    @with_retry()
    async def get_active_sessions_for_agent(
        self,
        agent_id: str,
    ) -> list[AgentSessionRecord]:
        """Fetch all active sessions for a specific agent."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM agent_sessions WHERE agent_id = $1 AND status = 'active'",
                agent_id,
            )
            return [AgentSessionRecord.from_dict(dict(row)) for row in rows]

    # -- Approval request CRUD --------------------------------------------

    @with_retry()
    async def insert_approval_request(self, record: ApprovalRequestRecord) -> None:
        """Insert an approval request record into the database."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO approval_requests (
                    request_id, fork_id, status, created_at, decided_at, decision
                ) VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (request_id) DO NOTHING
                """,
                record.request_id,
                record.fork_id,
                record.status,
                record.created_at,
                record.decided_at,
                record.decision,
            )

    @with_retry()
    async def get_approval_request(
        self,
        request_id: str,
    ) -> ApprovalRequestRecord | None:
        """Fetch an approval request record by ID."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM approval_requests WHERE request_id = $1", request_id
            )
            if row is None:
                return None
            return ApprovalRequestRecord.from_dict(dict(row))

    @with_retry()
    async def update_approval_decision(
        self,
        request_id: str,
        decision: str,
        decided_at: int,
        status: str,
    ) -> bool:
        """Update the decision for an approval request. Returns True if updated."""
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE approval_requests
                SET decision = $2, decided_at = $3, status = $4
                WHERE request_id = $1
                """,
                request_id,
                decision,
                decided_at,
                status,
            )
            return result.split()[-1] != "0"

    @with_retry()
    async def get_pending_approvals(self) -> list[ApprovalRequestRecord]:
        """Fetch all approval requests with status 'pending'."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM approval_requests WHERE status = $1", "pending"
            )
            return [ApprovalRequestRecord.from_dict(dict(row)) for row in rows]

    # -- Audit log CRUD ---------------------------------------------------

    @with_retry()
    async def insert_audit_log(self, record: AuditLogRecord) -> None:
        """Insert an audit log record into the database."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO audit_logs (log_id, agent_id, action, resource, timestamp, metadata)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                record.log_id,
                record.agent_id,
                record.action,
                record.resource,
                record.timestamp,
                json.dumps(record.metadata) if record.metadata else None,
            )

    @with_retry()
    async def get_audit_logs_for_agent(
        self,
        agent_id: str,
        limit: int = 100,
    ) -> list[AuditLogRecord]:
        """Fetch audit logs for a specific agent, ordered by timestamp desc."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM audit_logs WHERE agent_id = $1
                ORDER BY timestamp DESC LIMIT $2
                """,
                agent_id,
                limit,
            )
            return [AuditLogRecord.from_dict(dict(row)) for row in rows]


async def init_storage(
    database_url: str,
    pool_size: int = 5,
) -> StorageBackend:
    """Create and initialise the storage backend.

    Args:
        database_url: PostgreSQL connection string.
        pool_size: Number of connections in the pool.

    Returns:
        An initialised StorageBackend with tables created.
    """
    pool = await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=pool_size)
    backend = StorageBackend(pool)
    await backend.init_tables()
    return backend
