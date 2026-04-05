"""Agentic runtime for managing agent lifecycle and execution.

Provides the core runtime that orchestrates agent registration,
execution, and lifecycle management.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any

from src.agentic.schema import AgentDefinition, AgentContext
from src.agentic.registry import AgentRegistry
from src.agentic.executor import AgentExecutor
from src.agentic.policy import PolicyEngine
from src.agentic.approval import ApprovalGate
from src.agentic.context_store import ContextStore
from src.agentic.audit import AuditLogger
from src.agentic.mcp_manager import MCPManager
from src.storage.pg import StorageBackend
from src.orc.redis_bridge import RedisBridge

logger = logging.getLogger(__name__)


class AgentRuntime:
    """Main runtime for managing agent sessions and execution.

    Coordinates between registry, executor, policy engine, and
    supporting services to provide a complete agent runtime.

    Args:
        storage: PostgreSQL storage backend for persistence.
        redis: Redis bridge for pub/sub and caching.
        policy_engine: Policy engine for access control.
    """

    def __init__(
        self,
        storage: StorageBackend,
        redis: RedisBridge,
        policy_engine: PolicyEngine | None = None,
    ) -> None:
        self._storage = storage
        self._redis = redis
        self._policy = policy_engine or PolicyEngine()
        self._registry = AgentRegistry(redis)
        self._executor = AgentExecutor(storage, redis, self._policy)
        self._approval = ApprovalGate(storage, redis)
        self._context_store = ContextStore(redis)
        self._audit = AuditLogger(storage)
        self._mcp = MCPManager()
        self._running_sessions: dict[str, asyncio.Task] = {}

    async def register_agent(
        self,
        definition: AgentDefinition,
        agent_id: str | None = None,
    ) -> str:
        """Register an agent with the runtime.

        Args:
            definition: Agent definition containing configuration.
            agent_id: Optional specific agent ID. Generated if not provided.

        Returns:
            The agent_id assigned to this agent.
        """
        if agent_id is None:
            agent_id = f"agent-{uuid.uuid4().hex[:8]}"

        await self._registry.register(agent_id, definition)
        await self._audit.log(
            agent_id=agent_id,
            action="register",
            resource=f"agent:{agent_id}",
            metadata={"name": definition.name, "version": definition.version},
        )
        logger.info("Registered agent %s (%s v%s)", agent_id, definition.name, definition.version)
        return agent_id

    async def unregister_agent(self, agent_id: str) -> None:
        """Unregister an agent from the runtime.

        Args:
            agent_id: The agent to unregister.
        """
        await self._registry.unregister(agent_id)
        await self._audit.log(
            agent_id=agent_id,
            action="unregister",
            resource=f"agent:{agent_id}",
        )
        logger.info("Unregistered agent %s", agent_id)

    async def start_session(
        self,
        hub_id: str,
        agent_id: str,
        run_id: str,
        fork_id: str | None = None,
    ) -> str:
        """Start a new agent session.

        Args:
            hub_id: Hub this session belongs to.
            agent_id: Agent to run.
            run_id: Run identifier.
            fork_id: Optional fork context.

        Returns:
            The session_id for this session.
        """
        session_id = f"session-{uuid.uuid4().hex[:8]}"
        started_at = int(time.time() * 1000)

        # Create session record
        from src.storage.models import AgentSessionRecord
        record = AgentSessionRecord(
            session_id=session_id,
            hub_id=hub_id,
            agent_id=agent_id,
            run_id=run_id,
            status="active",
            started_at=started_at,
        )
        await self._storage.insert_agent_session(record)

        # Store context in Redis
        context = AgentContext(
            session_id=session_id,
            hub_id=hub_id,
            agent_id=agent_id,
            run_id=run_id,
            fork_id=fork_id,
        )
        await self._context_store.set(session_id, context)

        await self._audit.log(
            agent_id=agent_id,
            action="session_start",
            resource=f"session:{session_id}",
            metadata={"hub_id": hub_id, "run_id": run_id},
        )
        logger.info("Started session %s for agent %s", session_id, agent_id)
        return session_id

    async def end_session(self, session_id: str, status: str = "completed") -> None:
        """End an agent session.

        Args:
            session_id: Session to end.
            status: Final status (completed, failed, cancelled).
        """
        context = await self._context_store.get(session_id)
        if context is None:
            logger.warning("Session %s not found in context store", session_id)
            return

        ended_at = int(time.time() * 1000)
        await self._storage.update_agent_session_status(session_id, status, ended_at)
        await self._context_store.delete(session_id)

        await self._audit.log(
            agent_id=context.agent_id,
            action="session_end",
            resource=f"session:{session_id}",
            metadata={"status": status, "duration_ms": ended_at},
        )
        logger.info("Ended session %s with status %s", session_id, status)

    async def heartbeat(self, agent_id: str) -> None:
        """Record a heartbeat for an agent.

        Args:
            agent_id: Agent sending the heartbeat.
        """
        await self._registry.heartbeat(agent_id)

    async def list_active_agents(self) -> list[str]:
        """List all currently active agents.

        Returns:
            List of active agent IDs.
        """
        return await self._registry.list_active()

    async def check_policy(
        self,
        agent_id: str,
        action: str,
        resource: str,
    ) -> bool:
        """Check if an agent is allowed to perform an action.

        Args:
            agent_id: Agent requesting the action.
            action: Action being requested.
            resource: Resource being accessed.

        Returns:
            True if the action is allowed.
        """
        return await self._policy.check(agent_id, action, resource)

    @property
    def registry(self) -> AgentRegistry:
        """Access the agent registry."""
        return self._registry

    @property
    def executor(self) -> AgentExecutor:
        """Access the agent executor."""
        return self._executor

    @property
    def approval(self) -> ApprovalGate:
        """Access the approval gate."""
        return self._approval

    @property
    def context_store(self) -> ContextStore:
        """Access the context store."""
        return self._context_store

    @property
    def audit(self) -> AuditLogger:
        """Access the audit logger."""
        return self._audit

    @property
    def mcp(self) -> MCPManager:
        """Access the MCP manager."""
        return self._mcp


__all__ = ["AgentRuntime"]
