"""Retry utilities for async storage operations.

Provides a configurable retry decorator that can wrap any async
function (typically database calls) with exponential back-off and
jitter. This fills the gap noted in pg.py where asyncpg retry
logic was previously out of scope.

Also provides a circuit-breaker that guards repeated calls to a
failing dependency.  The breaker has three states:

* **closed**   – normal operation; failures are counted.
* **open**     – calls are short-circuited with ``CircuitOpenError``
                 until a cooldown expires.
* **half-open** – after the cooldown a single probe call is allowed.
                  If it succeeds the breaker returns to *closed*;
                  otherwise it re-opens.
"""

from __future__ import annotations

import asyncio
import enum
import functools
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


# ======================================================================
# Retry
# ======================================================================


@dataclass
class RetryConfig:
    """Configuration for retry behaviour.

    Attributes:
        max_attempts: Maximum number of attempts (including the first).
        base_delay: Base delay in seconds between retries.
        max_delay: Maximum delay cap in seconds.
        exponential_base: Base for exponential back-off calculation.
        jitter: Whether to add random jitter to delays.
        retryable_exceptions: Tuple of exception types that trigger a retry.
    """

    max_attempts: int = 3
    base_delay: float = 0.1
    max_delay: float = 5.0
    exponential_base: float = 2.0
    jitter: bool = True
    retryable_exceptions: tuple[type[Exception], ...] = (
        ConnectionError,
        OSError,
    )


def _calculate_delay(
    attempt: int, config: RetryConfig
) -> float:
    """Calculate the delay for a given retry attempt.

    Uses exponential back-off with optional jitter.
    """
    delay = config.base_delay * (
        config.exponential_base ** attempt
    )
    delay = min(delay, config.max_delay)
    if config.jitter:
        delay = random.uniform(0, delay)
    return delay


def with_retry(
    config: RetryConfig | None = None,
) -> Callable[
    [Callable[..., Coroutine[Any, Any, T]]],
    Callable[..., Coroutine[Any, Any, T]],
]:
    """Decorator that adds retry logic to an async function.

    Wraps the decorated function so that if it raises one of the
    configured retryable exceptions, it will be retried with
    exponential back-off up to ``max_attempts`` times.

    Args:
        config: Retry configuration. Defaults to RetryConfig().

    Returns:
        A decorator that wraps async functions with retry logic.
    """
    if config is None:
        config = RetryConfig()

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
                        delay = _calculate_delay(attempt, config)
                        logger.warning(
                            "Retry attempt %d/%d for %s after %s: %s",
                            attempt + 1,
                            config.max_attempts,
                            func.__name__,
                            type(exc).__name__,
                            exc,
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.error(
                            "All %d retry attempts exhausted for %s",
                            config.max_attempts,
                            func.__name__,
                        )
            raise last_exception  # type: ignore[misc]

        return wrapper

    return decorator


# ======================================================================
# Circuit Breaker
# ======================================================================


class CircuitState(enum.Enum):
    """Possible states of a circuit breaker."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when a call is rejected because the circuit is open."""


@dataclass
class CircuitBreakerConfig:
    """Configuration for the circuit breaker.

    Attributes:
        failure_threshold: Number of consecutive failures before the
            circuit opens.
        cooldown_seconds: How long (seconds) the circuit stays open
            before transitioning to half-open.
        half_open_max_calls: Maximum number of probe calls allowed in
            the half-open state.
    """

    failure_threshold: int = 5
    cooldown_seconds: float = 30.0
    half_open_max_calls: int = 1


class CircuitBreaker:
    """Async circuit breaker with closed / open / half-open states.

    Usage::

        breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))

        @breaker
        async def call_db():
            ...

        # If call_db fails 3 times consecutively the circuit opens and
        # subsequent calls immediately raise CircuitOpenError until the
        # cooldown expires.
    """

    def __init__(self, config: CircuitBreakerConfig | None = None) -> None:
        self._config = config or CircuitBreakerConfig()
        self._state: CircuitState = CircuitState.CLOSED
        self._failure_count: int = 0
        self._opened_at: float = 0.0
        self._half_open_calls: int = 0

    # -- public properties ------------------------------------------------

    @property
    def state(self) -> CircuitState:
        """Current state, auto-transitioning from OPEN when cooldown elapses."""
        if self._state is CircuitState.OPEN:
            elapsed = time.monotonic() - self._opened_at
            if elapsed >= self._config.cooldown_seconds:
                self._state = CircuitState.HALF_OPEN
                self._half_open_calls = 0
                logger.info("Circuit breaker transitioning to half-open")
        return self._state

    @property
    def failure_count(self) -> int:
        """Number of consecutive failures in the current closed cycle."""
        return self._failure_count

    # -- state transitions ------------------------------------------------

    def _record_success(self) -> None:
        if self._state is CircuitState.HALF_OPEN:
            logger.info("Probe succeeded — circuit closing")
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._half_open_calls = 0

    def _record_failure(self) -> None:
        self._failure_count += 1
        if self._state is CircuitState.HALF_OPEN:
            # Probe failed — re-open immediately.
            logger.warning("Probe failed — circuit re-opening")
            self._state = CircuitState.OPEN
            self._opened_at = time.monotonic()
            self._half_open_calls = 0
            return
        if self._failure_count >= self._config.failure_threshold:
            logger.warning(
                "Failure threshold reached (%d) — circuit opening",
                self._failure_count,
            )
            self._state = CircuitState.OPEN
            self._opened_at = time.monotonic()

    # -- decorator --------------------------------------------------------

    def __call__(
        self,
        func: Callable[..., Coroutine[Any, Any, T]],
    ) -> Callable[..., Coroutine[Any, Any, T]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            current = self.state  # triggers auto-transition
            if current is CircuitState.OPEN:
                raise CircuitOpenError(
                    f"Circuit is open; calls are blocked for "
                    f"{self._config.cooldown_seconds}s"
                )
            if current is CircuitState.HALF_OPEN:
                if self._half_open_calls >= self._config.half_open_max_calls:
                    raise CircuitOpenError(
                        "Circuit is half-open; max probe calls reached"
                    )
                self._half_open_calls += 1

            try:
                result = await func(*args, **kwargs)
            except Exception:
                self._record_failure()
                raise
            else:
                self._record_success()
                return result

        return wrapper

    # -- manual control ---------------------------------------------------

    def reset(self) -> None:
        """Manually reset the breaker to closed state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._half_open_calls = 0
