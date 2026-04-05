"""Context store for agent session data.

Manages agent context using Redis with TTL-based expiration.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from src.agentic.schema import AgentContext
from src.orc.redis_bridge import RedisBridge, with_redis_retry

logger = logging.getLogger(__name__)

# Redis key pattern
KEY_CONTEXT = "ctx:{fork_id}"  # TTL=3600


class ContextStore:
    """Redis-backed store for agent context data.

    Provides fast access to session context with automatic expiration.

    Args:
        redis: Redis bridge for storage.
    """

    def __init__(self, redis: RedisBridge) -> None:
        self._redis = redis

    def _key(self, session_id: str) -> str:
        """Generate the context key for a session."""
        return KEY_CONTEXT.format(fork_id=session_id)

    @with_redis_retry()
    async def set(
        self,
        session_id: str,
        context: AgentContext,
        ttl_seconds: int = 3600,
    ) -> None:
        """Store context for a session.

        Args:
            session_id: Session identifier.
            context: Context to store.
            ttl_seconds: Time-to-live in seconds.
        """
        client = self._redis.client
        if client is None:
            raise ConnectionError("Redis client not connected")

        key = self._key(session_id)
        await client.set(
            key,
            json.dumps(context.model_dump()),
            ex=ttl_seconds,
        )
        logger.debug("Stored context for session %s", session_id)

    @with_redis_retry()
    async def get(self, session_id: str) -> AgentContext | None:
        """Retrieve context for a session.

        Args:
            session_id: Session to look up.

        Returns:
            Context if found, None otherwise.
        """
        client = self._redis.client
        if client is None:
            raise ConnectionError("Redis client not connected")

        key = self._key(session_id)
        data = await client.get(key)
        if data is None:
            return None

        return AgentContext(**json.loads(data))

    @with_redis_retry()
    async def update(
        self,
        session_id: str,
        updates: dict[str, Any],
    ) -> bool:
        """Update specific fields in a session's context.

        Args:
            session_id: Session to update.
            updates: Fields to update.

        Returns:
            True if updated, False if session not found.
        """
        context = await self.get(session_id)
        if context is None:
            return False

        # Apply updates
        for key, value in updates.items():
            if hasattr(context, key):
                setattr(context, key, value)

        # Re-store with same TTL
        await self.set(session_id, context)
        return True

    @with_redis_retry()
    async def delete(self, session_id: str) -> None:
        """Delete context for a session.

        Args:
            session_id: Session to delete.
        """
        client = self._redis.client
        if client is None:
            raise ConnectionError("Redis client not connected")

        key = self._key(session_id)
        await client.delete(key)
        logger.debug("Deleted context for session %s", session_id)

    @with_redis_retry()
    async def exists(self, session_id: str) -> bool:
        """Check if context exists for a session.

        Args:
            session_id: Session to check.

        Returns:
            True if context exists.
        """
        client = self._redis.client
        if client is None:
            raise ConnectionError("Redis client not connected")

        key = self._key(session_id)
        return await client.exists(key) > 0


__all__ = ["ContextStore"]
