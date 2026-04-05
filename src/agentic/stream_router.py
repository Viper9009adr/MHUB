"""Stream router for agentic runtime with backpressure handling.

Routes messages between agents and handlers with flow control
to prevent overwhelming consumers.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any

logger = logging.getLogger(__name__)

# Backpressure thresholds
MAX_QUEUE_SIZE = 1000
HIGH_WATERMARK = 800
LOW_WATERMARK = 200


class BackpressureError(Exception):
    """Raised when backpressure limits are exceeded."""


class AgenticStreamRouter:
    """Routes bidirectional streams with backpressure handling.

    Maintains per-agent queues with size limits and provides
    flow control to prevent memory exhaustion.

    Attributes:
        max_queue_size: Maximum messages per queue.
        high_watermark: Threshold for applying backpressure.
        low_watermark: Threshold for releasing backpressure.
    """

    def __init__(
        self,
        max_queue_size: int = MAX_QUEUE_SIZE,
        high_watermark: int = HIGH_WATERMARK,
        low_watermark: int = LOW_WATERMARK,
    ) -> None:
        self._queues: dict[str, dict[str, asyncio.Queue]] = defaultdict(dict)
        self._backpressure: dict[str, bool] = {}
        self._lock = asyncio.Lock()
        self._max_queue_size = max_queue_size
        self._high_watermark = high_watermark
        self._low_watermark = low_watermark

    async def register(
        self,
        session_id: str,
        agent_id: str,
    ) -> asyncio.Queue:
        """Register a handler for a session + agent.

        Args:
            session_id: Session identifier.
            agent_id: Agent identifier.

        Returns:
            Queue for receiving messages.
        """
        async with self._lock:
            queue: asyncio.Queue = asyncio.Queue(maxsize=self._max_queue_size)
            self._queues[session_id][agent_id] = queue
            logger.debug(
                "Registered stream handler: session=%s agent=%s",
                session_id,
                agent_id,
            )
            return queue

    async def unregister(self, session_id: str, agent_id: str) -> None:
        """Unregister a handler.

        Args:
            session_id: Session identifier.
            agent_id: Agent identifier.
        """
        async with self._lock:
            if session_id in self._queues:
                self._queues[session_id].pop(agent_id, None)
                if not self._queues[session_id]:
                    del self._queues[session_id]
            logger.debug(
                "Unregistered stream handler: session=%s agent=%s",
                session_id,
                agent_id,
            )

    async def route(
        self,
        session_id: str,
        agent_id: str,
        message: Any,
    ) -> bool:
        """Route a message to a specific agent.

        Applies backpressure if queue is above high watermark.

        Args:
            session_id: Session identifier.
            agent_id: Target agent.
            message: Message to route.

        Returns:
            True if routed successfully.

        Raises:
            BackpressureError: If backpressure is active and queue is full.
        """
        async with self._lock:
            queues = self._queues.get(session_id, {})
            queue = queues.get(agent_id)
            if queue is None:
                logger.warning(
                    "No handler for session=%s agent=%s",
                    session_id,
                    agent_id,
                )
                return False

            # Check backpressure
            queue_size = queue.qsize()
            key = f"{session_id}:{agent_id}"
            if queue_size >= self._high_watermark:
                self._backpressure[key] = True
                logger.warning(
                    "Backpressure activated: session=%s agent=%s size=%d",
                    session_id,
                    agent_id,
                    queue_size,
                )

            if self._backpressure.get(key, False):
                if queue_size >= self._max_queue_size:
                    raise BackpressureError(
                        f"Queue full for {session_id}/{agent_id}"
                    )
            else:
                if queue_size <= self._low_watermark:
                    self._backpressure.pop(key, None)

            await queue.put(message)
            return True

    async def broadcast(
        self,
        session_id: str,
        message: Any,
    ) -> int:
        """Broadcast a message to all agents in a session.

        Args:
            session_id: Session to broadcast to.
            message: Message to broadcast.

        Returns:
            Number of agents that received the message.
        """
        async with self._lock:
            queues = dict(self._queues.get(session_id, {}))
            count = 0
            for agent_id, queue in queues.items():
                try:
                    queue.put_nowait(message)
                    count += 1
                except asyncio.QueueFull:
                    logger.warning(
                        "Queue full during broadcast: session=%s agent=%s",
                        session_id,
                        agent_id,
                    )
            return count

    async def get_queue_size(
        self,
        session_id: str,
        agent_id: str,
    ) -> int:
        """Get the current queue size for an agent.

        Args:
            session_id: Session identifier.
            agent_id: Agent identifier.

        Returns:
            Current queue size, 0 if not found.
        """
        async with self._lock:
            queues = self._queues.get(session_id, {})
            queue = queues.get(agent_id)
            return queue.qsize() if queue else 0

    async def get_active_sessions(self) -> list[str]:
        """Get list of active session IDs.

        Returns:
            List of session IDs with registered handlers.
        """
        async with self._lock:
            return list(self._queues.keys())

    async def get_agents(self, session_id: str) -> list[str]:
        """Get list of agents registered for a session.

        Args:
            session_id: Session to query.

        Returns:
            List of agent IDs.
        """
        async with self._lock:
            return list(self._queues.get(session_id, {}).keys())

    @property
    def is_backpressure_active(self) -> bool:
        """Check if any backpressure is currently active."""
        return any(self._backpressure.values())


__all__ = ["AgenticStreamRouter", "BackpressureError"]
