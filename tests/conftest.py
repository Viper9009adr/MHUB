from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
import sys

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def make_clock() -> Callable[[Sequence[int]], Callable[[], int]]:
    def factory(timeline: Sequence[int]) -> Callable[[], int]:
        if not timeline:
            raise ValueError("timeline must include at least one tick")
        ticks: Iterator[int] = iter(timeline)
        last = timeline[-1]

        def read_tick() -> int:
            nonlocal last
            try:
                last = next(ticks)
            except StopIteration:
                pass
            return last

        return read_tick

    return factory


@pytest.fixture
def grpc_test_port() -> int:
    """Return a port number for gRPC test servers."""
    return 0  # OS-assigned port
