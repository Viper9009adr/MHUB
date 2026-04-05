"""Redis bridge for pub/sub communication.

Provides async Redis connectivity using redis.asyncio with
automatic retry on connection errors and timeouts.
"""

from __future__ import annotations

import asyncio
import functools
import logging
from typing import Any, Callable, Coroutine, TypeVar

import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from src.storage.retry import RetryConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")

DEFAULT_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=0.1,
    max_delay=5.0,
    retryable_exceptions=(
        RedisConnectionError,
        RedisTimeoutError,
    ),
)


def with_redis_retry(
    config: RetryConfig | None = None,
) -> Callable[
    [Callable[..., Coroutine[Any, Any, T]]],
    Callable[..., Coroutine[Any, Any, T]],
]:
    """Decorator that adds retry logic for Redis-specific exceptions.

    Retries on redis.ConnectionError and redis.TimeoutError with
    exponential back-off.

    Args:
        config: Retry configuration. Defaults to DEFAULT_RETRY_CONFIG.

    Returns:
        A decorator that wraps async functions with Redis retry logic.
    """
    if config is None:
        config = DEFAULT_RETRY_CONFIG

    def decorator(
        func: Callable[..., Coroutine[Any, Any, T]],
    ) -> Callable[..., Coroutine[Any, Any, T]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception: Exception | None = None
            for attempt in range(config.max_attempts):
                try:
                    return await func(*args, **kwargs)
                except config.retryable_exceptions as exc:
                    last_exception = exc
                    if attempt < config.max_attempts - 1:
                        delay = config.base_delay * (
                            config.exponential_base ** attempt
                        )
                        delay = min(delay, config.max_delay)
                        if config.jitter:
                            import random
                            delay = random.uniform(0, delay)
                        logger.warning(
                            "Redis retry attempt %d/%d for %s after %s: %s",
                            attempt + 1,
                            config.max_attempts,
                            func.__name__,
                            type(exc).__name__,
                            exc,
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.error(
                            "All %d Redis retry attempts exhausted for %s",
                            config.max_attempts,
                            func.__name__,
                        )
            raise last_exception  # type: ignore[misc]

        return wrapper

    return decorator


class RedisBridge:
    """Async Redis bridge for pub/sub operations.

    Manages a single connection pool shared across the daemon.
    """

    def __init__(self, url: str = "redis://localhost:6379/0") -> None:
        self._url = url
        self._pool: aioredis.ConnectionPool | None = None
        self._client: aioredis.Redis | None = None
        self._pubsubs: dict[str, aioredis.client.PubSub] = {}

    async def connect(self) -> None:
        """Create the Redis connection pool and client."""
        self._pool = aioredis.ConnectionPool.from_url(self._url)
        self._client = aioredis.Redis(connection_pool=self._pool)
        logger.info("Redis bridge connected to %s", self._url)

    async def disconnect(self) -> None:
        """Close the Redis connection pool and client."""
        for channel, pubsub in self._pubsubs.items():
            try:
                await pubsub.close()
            except Exception:
                logger.exception("Error closing pubsub for channel %s", channel)
        self._pubsubs.clear()
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        if self._pool is not None:
            await self._pool.disconnect()
            self._pool = None
        logger.info("Redis bridge disconnected")

    @with_redis_retry()
    async def publish(self, channel: str, msg: bytes) -> None:
        """Publish a message to a Redis channel.

        Args:
            channel: The Redis channel name.
            msg: The message payload as bytes.
        """
        if self._client is None:
            raise ConnectionError("Redis client not connected")
        await self._client.publish(channel, msg)

    async def subscribe(self, channel: str) -> asyncio.Queue:
        """Subscribe to a Redis channel and return a queue of messages.

        Args:
            channel: The Redis channel name.

        Returns:
            An asyncio.Queue that yields message data bytes.
        """
        if self._client is None:
            raise ConnectionError("Redis client not connected")
        if channel in self._pubsubs:
            raise ValueError(f"Already subscribed to channel: {channel}")
        pubsub = self._client.pubsub()
        await pubsub.subscribe(channel)
        self._pubsubs[channel] = pubsub
        queue: asyncio.Queue = asyncio.Queue()

        async def _reader() -> None:
            try:
                async for message in pubsub.listen():
                    if message["type"] == "message":
                        await queue.put(message["data"])
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.error("Redis subscription error on %s: %s", channel, exc)

        task = asyncio.create_task(_reader())
        queue._reader_task = task  # type: ignore[attr-defined]
        return queue

    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from a Redis channel.

        Args:
            channel: The Redis channel name.
        """
        pubsub = self._pubsubs.pop(channel, None)
        if pubsub is not None:
            await pubsub.unsubscribe(channel)
            await pubsub.close()

    @property
    def client(self) -> aioredis.Redis | None:
        """Access the underlying Redis client."""
        return self._client

    async def publish_json(self, channel: str, data: Any) -> None:
        """Publish a JSON-serialisable object to a Redis channel.

        Args:
            channel: The Redis channel name.
            data: Any JSON-serialisable object.
        """
        import json
        payload = json.dumps(data).encode("utf-8")
        await self.publish(channel, payload)

    async def subscribe_json(self, channel: str) -> asyncio.Queue:
        """Subscribe to a Redis channel and yield parsed JSON objects.

        Args:
            channel: The Redis channel name.

        Returns:
            An asyncio.Queue that yields parsed JSON objects (dict/list).
        """
        import json
        raw_queue = await self.subscribe(channel)
        json_queue: asyncio.Queue = asyncio.Queue()

        async def _parser() -> None:
            try:
                while True:
                    raw = await raw_queue.get()
                    try:
                        parsed = json.loads(raw)
                        await json_queue.put(parsed)
                    except (json.JSONDecodeError, TypeError) as exc:
                        logger.warning(
                            "Failed to parse JSON from channel %s: %s",
                            channel,
                            exc,
                        )
            except asyncio.CancelledError:
                pass

        task = asyncio.create_task(_parser())
        json_queue._reader_task = task  # type: ignore[attr-defined]
        return json_queue


__all__ = ["RedisBridge", "with_redis_retry"]
