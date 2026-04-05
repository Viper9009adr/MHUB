"""Agent registry for tracking active agents.

Maintains registration state and heartbeats for agents using Redis
with TTL-based expiration.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from src.agentic.schema import AgentDefinition
from src.orc.redis_bridge import RedisBridge, with_redis_retry

logger = logging.getLogger(__name__)

# Redis key patterns with namespacing
KEY_ACTIVE = "ag:active:{agent_id}"  # TTL=30s
KEY_HEARTBEAT = "ag:heartbeat:{agent_id}"  # TTL=60s
KEY_SESSION = "ag:session:{session_id}"  # TTL=86400
KEY_DEFINITION = "ag:def:{agent_id}"  # TTL=3600


class AgentRegistry:
    """Registry for tracking active agents with Redis-backed storage.

    Uses Redis keys with TTL for automatic expiration of stale entries.
    Heartbeats must be sent periodically to maintain active status.

    Args:
        redis: Redis bridge for storage operations.
    """

    def __init__(self, redis: RedisBridge) -> None:
        self._redis = redis

    def _key_active(self, agent_id: str) -> str:
        """Generate the active key for an agent."""
        return KEY_ACTIVE.format(agent_id=agent_id)

    def _key_heartbeat(self, agent_id: str) -> str:
        """Generate the heartbeat key for an agent."""
        return KEY_HEARTBEAT.format(agent_id=agent_id)

    def _key_definition(self, agent_id: str) -> str:
        """Generate the definition key for an agent."""
        return KEY_DEFINITION.format(agent_id=agent_id)

    @with_redis_retry()
    async def register(self, agent_id: str, definition: AgentDefinition) -> None:
        """Register an agent with the registry.

        Stores the agent definition and marks it as active.

        Args:
            agent_id: Unique agent identifier.
            definition: Agent definition to store.
        """
        client = self._redis.client
        if client is None:
            raise ConnectionError("Redis client not connected")

        # Store definition
        def_key = self._key_definition(agent_id)
        await client.set(
            def_key,
            json.dumps(definition.model_dump()),
            ex=3600,  # 1 hour TTL
        )

        # Mark as active
        active_key = self._key_active(agent_id)
        await client.set(active_key, str(int(time.time() * 1000)), ex=30)

        # Set initial heartbeat
        hb_key = self._key_heartbeat(agent_id)
        await client.set(hb_key, str(int(time.time() * 1000)), ex=60)

        logger.debug("Registered agent %s", agent_id)

    @with_redis_retry()
    async def unregister(self, agent_id: str) -> None:
        """Remove an agent from the registry.

        Args:
            agent_id: Agent to unregister.
        """
        client = self._redis.client
        if client is None:
            raise ConnectionError("Redis client not connected")

        await client.delete(self._key_active(agent_id))
        await client.delete(self._key_heartbeat(agent_id))
        await client.delete(self._key_definition(agent_id))
        logger.debug("Unregistered agent %s", agent_id)

    @with_redis_retry()
    async def heartbeat(self, agent_id: str) -> None:
        """Record a heartbeat for an agent.

        Updates both the active and heartbeat keys with fresh TTLs.

        Args:
            agent_id: Agent sending the heartbeat.
        """
        client = self._redis.client
        if client is None:
            raise ConnectionError("Redis client not connected")

        now = str(int(time.time() * 1000))

        # Update active status (30s TTL)
        active_key = self._key_active(agent_id)
        await client.set(active_key, now, ex=30)

        # Update heartbeat (60s TTL)
        hb_key = self._key_heartbeat(agent_id)
        await client.set(hb_key, now, ex=60)

        logger.debug("Heartbeat for agent %s", agent_id)

    @with_redis_retry()
    async def is_active(self, agent_id: str) -> bool:
        """Check if an agent is currently active.

        Args:
            agent_id: Agent to check.

        Returns:
            True if the agent is active.
        """
        client = self._redis.client
        if client is None:
            raise ConnectionError("Redis client not connected")

        active_key = self._key_active(agent_id)
        return await client.exists(active_key) > 0

    @with_redis_retry()
    async def get_definition(self, agent_id: str) -> AgentDefinition | None:
        """Get the definition for an agent.

        Args:
            agent_id: Agent to look up.

        Returns:
            Agent definition if found, None otherwise.
        """
        client = self._redis.client
        if client is None:
            raise ConnectionError("Redis client not connected")

        def_key = self._key_definition(agent_id)
        data = await client.get(def_key)
        if data is None:
            return None

        return AgentDefinition(**json.loads(data))

    @with_redis_retry()
    async def list_active(self) -> list[str]:
        """List all currently active agents.

        Scans for keys matching the active pattern.

        Returns:
            List of active agent IDs.
        """
        client = self._redis.client
        if client is None:
            raise ConnectionError("Redis client not connected")

        pattern = "ag:active:*"
        keys = []
        cursor = 0
        while True:
            cursor, batch = await client.scan(cursor, match=pattern, count=100)
            keys.extend(batch)
            if cursor == 0:
                break

        # Extract agent_id from keys
        agent_ids = []
        for key in keys:
            key_str = key.decode() if isinstance(key, bytes) else key
            if key_str.startswith("ag:active:"):
                agent_ids.append(key_str[10:])

        return agent_ids


__all__ = ["AgentRegistry"]
