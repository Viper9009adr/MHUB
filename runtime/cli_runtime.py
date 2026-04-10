from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from .message_bus import Evt, MessageBus, Req, RunResult, now_ms
from .worker_runner import WorkerRunner


class CE(asyncio.CancelledError):
    """Raised when awaiting a cancelled runtime request."""

    pass


@dataclass(slots=True)
class _RunState:
    req: Req
    task: asyncio.Task[None] | None = None
    done: asyncio.Event = field(default_factory=asyncio.Event)
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    cancelled: bool = False
    code: int = 1
    ok: bool = False
    err: Exception | None = None
    events: list[Evt] = field(default_factory=list)


class CliRuntime:
    """Manage submitted CLI runs and expose their results."""

    def __init__(self, worker: WorkerRunner | None = None, bus: MessageBus | None = None) -> None:
        self._worker = worker or WorkerRunner()
        self._bus = bus or MessageBus()
        self._runs: dict[str, _RunState] = {}
        self._closed = False

    @property
    def bus(self) -> MessageBus:
        """Return the shared runtime message bus."""
        return self._bus

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("runtime closed")

    def _emit(self, rid: str, event: Evt) -> None:
        state = self._runs.get(rid)
        if state is not None:
            state.events.append(event)
        try:
            self._bus.emit(event)
        except RuntimeError:
            return

    async def _run(self, rid: str) -> None:
        state = self._runs[rid]
        try:
            state.code = await self._worker.run(
                req=state.req,
                cancel_event=state.cancel_event,
                emit=lambda evt: self._emit(rid, evt),
            )
            state.ok = state.code == 0 and not state.cancelled
        except Exception as exc:  # pragma: no cover - defensive boundary
            state.err = exc
            state.ok = False
            state.code = 1
            self._emit(
                rid,
                Evt(
                    topic="run",
                    kind="err",
                    rid=rid,
                    ts_ms=now_ms(),
                    payload={"msg": str(exc)},
                ),
            )
            self._emit(
                rid,
                Evt(
                    topic="run",
                    kind="exit",
                    rid=rid,
                    ts_ms=now_ms(),
                    payload={"code": state.code},
                ),
            )
        finally:
            state.done.set()

    def submit(self, req: Req) -> str:
        """Submit a request and start its background task."""
        self._require_open()
        if not req.rid or not req.cmd:
            raise ValueError("rid and cmd must be non-empty")
        if req.rid in self._runs:
            raise KeyError(req.rid)

        state = _RunState(req=req)
        self._runs[req.rid] = state
        queued = Evt(topic="run", kind="queued", rid=req.rid, ts_ms=now_ms(), payload={})
        self._emit(req.rid, queued)
        state.task = asyncio.create_task(self._run(req.rid))
        return req.rid

    def cancel(self, rid: str) -> bool:
        """Request cancellation for an active run id."""
        self._require_open()
        state = self._runs.get(rid)
        if state is None:
            raise KeyError(rid)

        if not state.cancelled:
            state.cancelled = True
            state.cancel_event.set()
            self._emit(
                rid,
                Evt(topic="run", kind="cancel", rid=rid, ts_ms=now_ms(), payload={}),
            )
        return True

    async def await_result(self, rid: str, timeout_ms: int = 0) -> RunResult:
        """Wait for completion and return the run result."""
        self._require_open()
        state = self._runs.get(rid)
        if state is None:
            raise KeyError(rid)

        if timeout_ms > 0:
            try:
                await asyncio.wait_for(state.done.wait(), timeout=timeout_ms / 1000)
            except asyncio.TimeoutError as exc:
                raise RuntimeError("timeout") from exc
        else:
            await state.done.wait()

        if state.cancelled:
            raise CE(rid)

        return RunResult(
            ok=state.ok,
            code=state.code,
            cancelled=state.cancelled,
            err=None,
            events=list(state.events),
        )

    def __getattr__(self, name: str):
        if name == "await":
            return self.await_result
        raise AttributeError(name)

    async def close(self) -> bool:
        """Cancel pending runs, emit done, and close the bus."""
        if self._closed:
            return True
        self._closed = True

        for rid, state in self._runs.items():
            if state.done.is_set():
                continue
            if not state.cancelled:
                state.cancelled = True
                state.cancel_event.set()
                self._emit(
                    rid,
                    Evt(topic="run", kind="cancel", rid=rid, ts_ms=now_ms(), payload={}),
                )

        pending_tasks = [
            state.task
            for state in self._runs.values()
            if state.task is not None and not state.done.is_set()
        ]
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)

        try:
            self._bus.emit(
                Evt(topic="sys", kind="done", rid="sys", ts_ms=now_ms(), payload={})
            )
        except RuntimeError:
            pass
        self._bus.close()
        return True
