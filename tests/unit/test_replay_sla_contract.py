from __future__ import annotations

from collections.abc import Callable, Sequence

import pytest
from src.agentic_cli.replay_sla import ReplaySLARuntime, run_replay_sla


def test_replays_bounded_backlog_within_sla(make_clock: Callable[[Sequence[int]], Callable[[], int]]) -> None:
    result = run_replay_sla(total=5000, batch_size=250, sla_ms=2000, now_ms=make_clock([0, 1500]))

    assert result.seen == 5000
    assert result.elapsed_ms == 1500
    assert result.within_sla is True


def test_keeps_replay_payload_stable_after_source_mutation() -> None:
    runtime = ReplaySLARuntime()
    payload = {"event": "sync", "key": "k-1"}

    runtime.append(payload)
    payload["key"] = "k-1-mutated"

    batch = runtime.replay(10)
    assert batch[0] == {"event": "sync", "key": "k-1"}


def test_runtime_drains_events_after_each_replay() -> None:
    runtime = ReplaySLARuntime()
    runtime.append({"event": "e-1"})
    runtime.append({"event": "e-2"})
    runtime.append({"event": "e-3"})

    assert runtime.replay(2) == [{"event": "e-1"}, {"event": "e-2"}]
    assert runtime.replay(2) == [{"event": "e-3"}]
    assert runtime.replay(2) == []


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"total": -1}, "total must be >= 0"),
        ({"batch_size": 0}, "batch_size must be > 0"),
        ({"sla_ms": -1}, "sla_ms must be >= 0"),
    ],
)
def test_replay_sla_rejects_invalid_bounds(kwargs: dict[str, int], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        run_replay_sla(**kwargs)


def test_runtime_rejects_non_positive_replay_limit() -> None:
    runtime = ReplaySLARuntime()
    with pytest.raises(ValueError, match="batch_size must be > 0"):
        runtime.replay(0)


def test_elapsed_ms_is_clamped_for_clock_rollback(make_clock: Callable[[Sequence[int]], Callable[[], int]]) -> None:
    result = run_replay_sla(total=1, batch_size=1, sla_ms=0, now_ms=make_clock([500, 200]))

    assert result.elapsed_ms == 0
    assert result.within_sla is True


def test_zero_total_stays_within_sla(make_clock: Callable[[Sequence[int]], Callable[[], int]]) -> None:
    result = run_replay_sla(total=0, batch_size=10, sla_ms=10, now_ms=make_clock((10, 15)))

    assert result.seen == 0
    assert result.elapsed_ms == 5
    assert result.within_sla is True
