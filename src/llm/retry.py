"""LLM retry decorator with exponential back-off.

Mirrors the pattern from src/storage/retry.py but specialised for
LLM provider exceptions (LLMRateLimitError, LLMTimeoutError).
"""

from __future__ import annotations

import asyncio
import functools
import logging
import random
from typing import Any, Callable, Coroutine, TypeVar

from src.llm.errors import LLMError, LLMRateLimitError, LLMTimeoutError

logger = logging.getLogger(__name__)

T = TypeVar("T")


class LLMRetryConfig:
    """Configuration for LLM retry behaviour."""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
    ) -> None:
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter


DEFAULT_LLM_RETRY_CONFIG = LLMRetryConfig(
    max_attempts=3,
    base_delay=1.0,
    max_delay=30.0,
)


def with_llm_retry(
    config: LLMRetryConfig | None = None,
) -> Callable[
    [Callable[..., Coroutine[Any, Any, T]]],
    Callable[..., Coroutine[Any, Any, T]],
]:
    """Decorator that adds retry logic for LLM-specific exceptions.

    Retries on LLMRateLimitError and LLMTimeoutError with exponential
    back-off. Other LLMError subclasses are NOT retried (e.g. auth
    errors, provider errors).

    Args:
        config: Retry configuration. Defaults to DEFAULT_LLM_RETRY_CONFIG.

    Returns:
        A decorator that wraps async functions with LLM retry logic.
    """
    if config is None:
        config = DEFAULT_LLM_RETRY_CONFIG

    retryable = (LLMRateLimitError, LLMTimeoutError)

    def decorator(
        func: Callable[..., Coroutine[Any, Any, T]],
    ) -> Callable[..., Coroutine[Any, Any, T]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception: Exception | None = None
            for attempt in range(config.max_attempts):
                try:
                    return await func(*args, **kwargs)
                except retryable as exc:
                    last_exception = exc
                    if attempt < config.max_attempts - 1:
                        delay = config.base_delay * (
                            config.exponential_base ** attempt
                        )
                        delay = min(delay, config.max_delay)
                        if config.jitter:
                            delay = random.uniform(0, delay)
                        logger.warning(
                            "LLM retry attempt %d/%d for %s after %s: %s",
                            attempt + 1,
                            config.max_attempts,
                            func.__name__,
                            type(exc).__name__,
                            exc,
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.error(
                            "All %d LLM retry attempts exhausted for %s",
                            config.max_attempts,
                            func.__name__,
                        )
            raise last_exception  # type: ignore[misc]

        return wrapper

    return decorator


__all__ = ["LLMRetryConfig", "with_llm_retry", "DEFAULT_LLM_RETRY_CONFIG"]
