"""Audit logger for agent actions.

Records all significant agent actions to the database for
compliance and debugging purposes.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any

from src.storage.pg import StorageBackend
from src.storage.models import AuditLogRecord
from src.storage.retry import with_retry

logger = logging.getLogger(__name__)


class AuditLogger:
    """Logger for recording agent actions to persistent storage.

    All actions are stored in the audit_logs table with timestamps
    and optional metadata.

    Args:
        storage: Storage backend for persistence.
    """

    def __init__(self, storage: StorageBackend) -> None:
        self._storage = storage

    @with_retry()
    async def log(
        self,
        agent_id: str,
        action: str,
        resource: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Record an audit log entry.

        Args:
            agent_id: Agent performing the action.
            action: Action being performed.
            resource: Resource being accessed.
            metadata: Optional additional context.

        Returns:
            The log_id for this entry.
        """
        log_id = f"log-{uuid.uuid4().hex[:8]}"
        timestamp = int(time.time() * 1000)

        record = AuditLogRecord(
            log_id=log_id,
            agent_id=agent_id,
            action=action,
            resource=resource,
            timestamp=timestamp,
            metadata=metadata or {},
        )
        await self._storage.insert_audit_log(record)

        logger.debug(
            "Audit log: agent=%s action=%s resource=%s",
            agent_id,
            action,
            resource,
        )
        return log_id

    async def get_logs(
        self,
        agent_id: str,
        limit: int = 100,
    ) -> list[AuditLogRecord]:
        """Retrieve audit logs for an agent.

        Args:
            agent_id: Agent to query.
            limit: Maximum number of logs to return.

        Returns:
            List of audit log records, newest first.
        """
        return await self._storage.get_audit_logs_for_agent(agent_id, limit)

    async def log_tool_call(
        self,
        agent_id: str,
        tool_name: str,
        session_id: str,
        success: bool,
        execution_ms: int,
    ) -> str:
        """Convenience method for logging tool calls.

        Args:
            agent_id: Agent making the call.
            tool_name: Tool being called.
            session_id: Session context.
            success: Whether the call succeeded.
            execution_ms: Execution time in milliseconds.

        Returns:
            The log_id for this entry.
        """
        return await self.log(
            agent_id=agent_id,
            action=f"tool_call:{tool_name}",
            resource=f"session:{session_id}",
            metadata={
                "success": success,
                "execution_ms": execution_ms,
            },
        )

    async def log_session_event(
        self,
        agent_id: str,
        session_id: str,
        event: str,
        details: dict[str, Any] | None = None,
    ) -> str:
        """Convenience method for logging session events.

        Args:
            agent_id: Agent involved.
            session_id: Session identifier.
            event: Event type (start, end, pause, etc.).
            details: Additional event details.

        Returns:
            The log_id for this entry.
        """
        return await self.log(
            agent_id=agent_id,
            action=f"session:{event}",
            resource=f"session:{session_id}",
            metadata=details,
        )


__all__ = ["AuditLogger"]
