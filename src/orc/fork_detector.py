"""Fork detector — subscribes to Redis divergence events and dispatches them.

Listens on the Redis channel for divergence detection events published
by HAL agents, validates payloads, and forwards them directly to the
ForkHandler (in-process, not via gRPC).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import msgpack

from src.orc.fork_handler import ForkHandler, DivergenceEvent
from src.orc.redis_bridge import RedisBridge

logger = logging.getLogger(__name__)

CHANNEL_DETECT = "meridian:fork:detect"


class ForkDetector:
    """Subscribes to Redis divergence events and dispatches to ForkHandler.

    The listen_for_divergence() method returns an asyncio.Task that
    can be cancelled for lifecycle management.

    Args:
        redis: RedisBridge instance for subscribing to channels.
        handler: ForkHandler instance for processing events.
    """

    def __init__(self, redis: RedisBridge, handler: ForkHandler) -> None:
        self._redis = redis
        self._handler = handler
        self._task: asyncio.Task | None = None

    def listen_for_divergence(self) -> asyncio.Task:
        """Start listening for divergence events on Redis.

        Returns:
            An asyncio.Task that runs the listener loop. Cancel this
            task to stop listening (e.g. during daemon shutdown).
        """
        self._task = asyncio.create_task(self._listen_loop())
        logger.info("ForkDetector listening on channel %s", CHANNEL_DETECT)
        return self._task

    async def stop(self) -> None:
        """Stop the listener task if running."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
            logger.info("ForkDetector stopped")

    async def _listen_loop(self) -> None:
        """Internal loop that reads from the Redis subscription queue."""
        try:
            queue = await self._redis.subscribe(CHANNEL_DETECT)
            while True:
                raw = await queue.get()
                await self._handle_raw_event(raw)
        except asyncio.CancelledError:
            logger.info("ForkDetector listener cancelled")
            raise
        except Exception as exc:
            logger.error("ForkDetector listener error: %s", exc)

    async def _handle_raw_event(self, raw: bytes) -> None:
        """Parse a raw Redis message and dispatch to ForkHandler.

        Args:
            raw: Raw bytes from the Redis subscription.
        """
        try:
            event = DivergenceEvent.from_msgpack(raw)
        except Exception as exc:
            logger.error("Failed to parse divergence event: %s", exc)
            return

        # Validate required fields
        if not event.parent_hub_id or not event.hal_agent_id:
            logger.warning("Incomplete divergence event, skipping: %s", event)
            return

        # Dispatch to ForkHandler (async call)
        try:
            await self._handler.handle_detection(event)
        except Exception as exc:
            logger.error("ForkHandler.handle_detection failed: %s", exc)

    async def handle_divergence_event(self, payload: bytes) -> None:
        """Handle a divergence event from an external source (e.g. gRPC).

        This is the renamed version of forward_to_orc. It parses the
        payload and dispatches to ForkHandler.

        Args:
            payload: Raw msgpack bytes of the divergence event.
        """
        await self._handle_raw_event(payload)


__all__ = ["ForkDetector"]
