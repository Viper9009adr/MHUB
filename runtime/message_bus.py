from __future__ import annotations

from dataclasses import dataclass, field
from time import time
from typing import Any


def now_ms() -> int:
    """Return current Unix time in milliseconds."""
    return int(time() * 1000)


@dataclass(slots=True)
class Req:
    """Run request envelope for the runtime worker."""
    rid: str
    cmd: str
    args: list[str] = field(default_factory=list)
    meta: dict[str, str] = field(default_factory=dict)
    timeout_ms: int = 0


@dataclass(slots=True)
class Err:
    """Structured error payload for runtime events."""
    type: str
    msg: str
    code: int = 0
    data: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Evt:
    """Runtime event frame emitted during execution."""
    topic: str
    kind: str
    rid: str
    ts_ms: int
    payload: dict[str, Any] = field(default_factory=dict)
    err: Err | None = None


@dataclass(slots=True)
class RunResult:
    """Final run result with exit code and collected events."""
    ok: bool
    code: int
    cancelled: bool = False
    err: Err | None = None
    events: list[Evt] = field(default_factory=list)


class MessageBus:
    """In-memory event bus for runtime execution events."""

    def __init__(self) -> None:
        self._events: list[Evt] = []
        self._closed = False

    @property
    def closed(self) -> bool:
        """Return whether this bus has been closed."""
        return self._closed

    def emit(self, event: Evt) -> None:
        """Append one event to the bus."""
        if self._closed:
            raise RuntimeError("bus closed")
        self._events.append(event)

    def events(self) -> list[Evt]:
        """Return a snapshot of buffered events."""
        return list(self._events)

    def close(self) -> bool:
        """Mark the bus closed and reject new events."""
        if self._closed:
            return True
        self._closed = True
        return True
