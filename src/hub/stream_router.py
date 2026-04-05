"""Stream router for gRPC bidirectional streaming.

Routes AgentMessage and OrchEvent streams to the correct handlers
based on run_id and agent_id. Includes backpressure handling to
prevent memory exhaustion.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any

logger = logging.getLogger(__name__)

# Backpressure thresholds
DEFAULT_MAX_QUEUE_SIZE = 1000
DEFAULT_HIGH_WATERMARK = 800
DEFAULT_LOW_WATERMARK = 200


class BackpressureError(Exception):
    """Raised when backpressure limits are exceeded."""


class StreamRouter:
    """Routes bidirectional gRPC streams to registered handlers.

    Maintains a mapping of run_id -> active stream handlers.
    When a message arrives, it is routed to the appropriate handler
    based on run_id and agent_id.

    Includes backpressure handling with configurable thresholds:
    - high_watermark: Queue size that triggers backpressure
    - low_watermark: Queue size that releases backpressure
    - max_queue_size: Maximum queue capacity
    """

    def __init__(
        self,
        max_queue_size: int = DEFAULT_MAX_QUEUE_SIZE,
        high_watermark: int = DEFAULT_HIGH_WATERMARK,
        low_watermark: int = DEFAULT_LOW_WATERMARK,
    ) -> None:
        self._handlers: dict[int, dict[str, asyncio.Queue]] = defaultdict(dict)
        self._lock = asyncio.Lock()
        self._backpressure: dict[str, bool] = {}
        self._max_queue_size = max_queue_size
        self._high_watermark = high_watermark
        self._low_watermark = low_watermark

    async def register_handler(
        self, run_id: int, agent_id: str, queue: asyncio.Queue | None = None
    ) -> asyncio.Queue:
        """Register a handler queue for a specific run_id + agent_id.

        Args:
            run_id: The run identifier.
            agent_id: The agent identifier.
            queue: Optional queue to use. If None, creates a bounded queue.

        Returns:
            The queue registered for this handler.
        """
        async with self._lock:
            if queue is None:
                queue = asyncio.Queue(maxsize=self._max_queue_size)
            self._handlers[run_id][agent_id] = queue
            logger.debug(
                "Registered handler: run_id=%d agent_id=%s", run_id, agent_id
            )
            return queue

    async def unregister_handler(self, run_id: int, agent_id: str) -> None:
        """Remove a handler registration.

        Args:
            run_id: The run identifier.
            agent_id: The agent identifier.
        """
        async with self._lock:
            if run_id in self._handlers:
                self._handlers[run_id].pop(agent_id, None)
                if not self._handlers[run_id]:
                    del self._handlers[run_id]
            # Clear backpressure state
            key = f"{run_id}:{agent_id}"
            self._backpressure.pop(key, None)
            logger.debug(
                "Unregistered handler: run_id=%d agent_id=%s", run_id, agent_id
            )

    async def route_message(self, run_id: int, agent_id: str, message: Any) -> bool:
        """Route a message to the registered handler for run_id + agent_id.

        Applies backpressure when queue exceeds high watermark.

        Args:
            run_id: The run identifier.
            agent_id: The target agent identifier.
            message: The message to route.

        Returns:
            True if the message was routed, False if no handler found.

        Raises:
            BackpressureError: If backpressure is active and queue is full.
        """
        async with self._lock:
            handlers = self._handlers.get(run_id, {})
            target = handlers.get(agent_id)
            if target is None:
                logger.warning(
                    "No handler for run_id=%d agent_id=%s", run_id, agent_id
                )
                return False

            # Check and apply backpressure
            queue_size = target.qsize()
            key = f"{run_id}:{agent_id}"

            if queue_size >= self._high_watermark:
                self._backpressure[key] = True
                logger.warning(
                    "Backpressure activated: run_id=%d agent_id=%s size=%d",
                    run_id,
                    agent_id,
                    queue_size,
                )

            if self._backpressure.get(key, False):
                if queue_size >= self._max_queue_size:
                    raise BackpressureError(
                        f"Queue full for run_id={run_id} agent_id={agent_id}"
                    )
                elif queue_size <= self._low_watermark:
                    self._backpressure.pop(key, None)
                    logger.debug(
                        "Backpressure released: run_id=%d agent_id=%s",
                        run_id,
                        agent_id,
                    )

            await target.put(message)
            return True

    async def broadcast(self, run_id: int, message: Any) -> int:
        """Broadcast a message to all handlers for a run_id.

        Uses put_nowait to avoid blocking, skips full queues.

        Args:
            run_id: The run identifier.
            message: The message to broadcast.

        Returns:
            Number of handlers that received the message.
        """
        async with self._lock:
            handlers = dict(self._handlers.get(run_id, {}))
            count = 0
            for agent_id, queue in handlers.items():
                try:
                    queue.put_nowait(message)
                    count += 1
                except asyncio.QueueFull:
                    logger.warning(
                        "Queue full during broadcast: run_id=%d agent_id=%s",
                        run_id,
                        agent_id,
                    )
            return count

    async def get_active_runs(self) -> list[int]:
        """Return list of run_ids with active handlers."""
        async with self._lock:
            return list(self._handlers.keys())

    async def get_agents_for_run(self, run_id: int) -> list[str]:
        """Return list of agent_ids registered for a run."""
        async with self._lock:
            return list(self._handlers.get(run_id, {}).keys())

    async def get_queue_size(self, run_id: int, agent_id: str) -> int:
        """Get the current queue size for a handler.

        Args:
            run_id: The run identifier.
            agent_id: The agent identifier.

        Returns:
            Current queue size, 0 if not found.
        """
        async with self._lock:
            handlers = self._handlers.get(run_id, {})
            queue = handlers.get(agent_id)
            return queue.qsize() if queue else 0

    @property
    def is_backpressure_active(self) -> bool:
        """Check if any backpressure is currently active."""
        return any(self._backpressure.values())


__all__ = ["StreamRouter", "BackpressureError"]
