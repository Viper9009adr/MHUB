from __future__ import annotations

import asyncio
from collections.abc import Callable

from .message_bus import Evt, Req, now_ms


class WorkerRunner:
    """Execute one command request and emit lifecycle events."""

    async def run(
        self,
        req: Req,
        cancel_event: asyncio.Event,
        emit: Callable[[Evt], None],
    ) -> int:
        """Run a subprocess for the request and return exit code."""
        emit(Evt(topic="run", kind="start", rid=req.rid, ts_ms=now_ms(), payload={}))
        try:
            proc = await asyncio.create_subprocess_exec(
                req.cmd,
                *req.args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            emit(
                Evt(
                    topic="run",
                    kind="err",
                    rid=req.rid,
                    ts_ms=now_ms(),
                    payload={},
                )
            )
            emit(
                Evt(
                    topic="run",
                    kind="exit",
                    rid=req.rid,
                    ts_ms=now_ms(),
                    payload={"code": 127},
                )
            )
            return 127

        async def _read_stream(stream: asyncio.StreamReader | None, kind: str) -> None:
            if stream is None:
                return
            while True:
                line = await stream.readline()
                if not line:
                    return
                emit(
                    Evt(
                        topic="run",
                        kind=kind,
                        rid=req.rid,
                        ts_ms=now_ms(),
                        payload={"line": line.decode(errors="replace").rstrip("\n")},
                    )
                )

        out_task = asyncio.create_task(_read_stream(proc.stdout, "out"))
        err_task = asyncio.create_task(_read_stream(proc.stderr, "err"))

        wait_task = asyncio.create_task(proc.wait())
        cancel_task = asyncio.create_task(cancel_event.wait())
        done, pending = await asyncio.wait(
            {wait_task, cancel_task}, return_when=asyncio.FIRST_COMPLETED
        )

        if cancel_task in done and cancel_event.is_set() and proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()

        if wait_task in pending:
            wait_task.cancel()
        if cancel_task in pending:
            cancel_task.cancel()

        await asyncio.gather(out_task, err_task, return_exceptions=True)
        code = int(proc.returncode if proc.returncode is not None else 1)
        emit(
            Evt(
                topic="run",
                kind="exit",
                rid=req.rid,
                ts_ms=now_ms(),
                payload={"code": code},
            )
        )
        return code
