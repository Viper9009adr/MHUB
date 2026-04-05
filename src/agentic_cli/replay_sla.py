"""Legacy replay SLA benchmark helpers kept alongside the new `run` CLI path."""

from __future__ import annotations

from dataclasses import dataclass
from time import time_ns
from typing import Callable, List, Mapping, Optional


ERR_TOTAL_BOUNDS = "total must be >= 0"
ERR_BATCH_SIZE_BOUNDS = "batch_size must be > 0"
ERR_SLA_BOUNDS = "sla_ms must be >= 0"

DEFAULT_TOTAL = 5000
DEFAULT_BATCH_SIZE = 250
DEFAULT_SLA_MS = 2000


def _require_int(value: object, name: str) -> int:
    """Return an integer input while rejecting bools and other types."""

    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int")
    return value


def _now_ms() -> int:
    """Return the current wall-clock time in whole milliseconds."""

    return time_ns() // 1_000_000


def _clock_ms(clock: Callable[[], int]) -> int:
    """Call a clock function and validate that it returns an integer."""

    return _require_int(clock(), "now_ms")


@dataclass(frozen=True)
class ReplaySLAResult:
    """Captures how many events replayed and whether the SLA was met."""

    seen: int
    elapsed_ms: int
    within_sla: bool


class ReplaySLARuntime:
    """Minimal in-memory event queue used by the replay SLA benchmark."""

    def __init__(self) -> None:
        """Start with an empty replay queue."""

        self._events: List[dict[str, str]] = []

    def append(self, values: Mapping[str, str]) -> None:
        """Append one event payload to the in-memory replay queue."""

        self._events.append(dict(values))

    def replay(self, limit: int) -> List[dict[str, str]]:
        """Pop up to `limit` queued events and return defensive copies."""

        limit = _require_int(limit, "batch_size")
        if limit <= 0:
            raise ValueError(ERR_BATCH_SIZE_BOUNDS)
        batch = self._events[:limit]
        del self._events[: len(batch)]
        return [dict(values) for values in batch]


def format_replay_sla_result(result: ReplaySLAResult) -> str:
    """Format a replay SLA result as the CLI's single-line status output."""

    within = "true" if result.within_sla else "false"
    return f"seen={result.seen} elapsed_ms={result.elapsed_ms} within_sla={within}"


def run_replay_sla(
    total: int = DEFAULT_TOTAL,
    batch_size: int = DEFAULT_BATCH_SIZE,
    sla_ms: int = DEFAULT_SLA_MS,
    now_ms: Optional[Callable[[], int]] = None,
) -> ReplaySLAResult:
    """Benchmark in-memory replay throughput against a bounded SLA target."""

    total = _require_int(total, "total")
    batch_size = _require_int(batch_size, "batch_size")
    sla_ms = _require_int(sla_ms, "sla_ms")
    if total < 0:
        raise ValueError(ERR_TOTAL_BOUNDS)
    if batch_size <= 0:
        raise ValueError(ERR_BATCH_SIZE_BOUNDS)
    if sla_ms < 0:
        raise ValueError(ERR_SLA_BOUNDS)

    clock = now_ms if now_ms is not None else _now_ms
    runtime = ReplaySLARuntime()

    started = _clock_ms(clock)
    seen = 0
    for idx in range(total):
        runtime.append({"event": f"e-{idx}"})
        if (idx + 1) % batch_size == 0:
            seen += len(runtime.replay(batch_size))

    while seen < total:
        batch = runtime.replay(batch_size)
        if not batch:
            break
        seen += len(batch)
    elapsed_ms = max(0, _clock_ms(clock) - started)
    return ReplaySLAResult(
        seen=seen,
        elapsed_ms=elapsed_ms,
        within_sla=seen == total and elapsed_ms <= sla_ms,
    )


__all__ = [
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_SLA_MS",
    "DEFAULT_TOTAL",
    "ERR_BATCH_SIZE_BOUNDS",
    "ERR_SLA_BOUNDS",
    "ERR_TOTAL_BOUNDS",
    "ReplaySLARuntime",
    "ReplaySLAResult",
    "format_replay_sla_result",
    "run_replay_sla",
]
