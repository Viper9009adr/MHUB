"""CLI entry point for the AGENTIC CLI Phase 1 Python surface."""

from __future__ import annotations

import argparse
import contextlib
import sys
from typing import Callable, Optional, Sequence, TextIO

from .config import AgenticCliConfig
from .contracts import RunCommand, format_run_receipt
from .errors import AgenticCliError
from .replay_sla import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_SLA_MS,
    DEFAULT_TOTAL,
    format_replay_sla_result,
    run_replay_sla,
)
from .service import AgenticCliService


def _looks_like_program_name(token: str, prog: str) -> bool:
    """Return whether the first argv token is the program name itself."""

    def _normalize(value: str) -> str:
        normalized = value.rsplit("/", maxsplit=1)[-1].rsplit("\\", maxsplit=1)[-1]
        lowered = normalized.casefold()
        if lowered.endswith(".exe"):
            return lowered[:-4]
        return lowered

    return _normalize(token) == _normalize(prog)


def build_parser() -> argparse.ArgumentParser:
    """Build the parser for the `run` and legacy `replay-sla` commands."""

    parser = argparse.ArgumentParser(prog=AgenticCliConfig().program_name)
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("prompt")
    run_parser.add_argument("--session-id")

    replay_sla = subparsers.add_parser("replay-sla")
    replay_sla.add_argument("--total", type=int, default=DEFAULT_TOTAL)
    replay_sla.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    replay_sla.add_argument("--sla-ms", type=int, default=DEFAULT_SLA_MS)

    subparsers.add_parser("tui")

    return parser


def main(
    argv: Optional[Sequence[str]] = None,
    out: Optional[TextIO] = None,
    now_ms: Optional[Callable[[], int]] = None,
    service: Optional[AgenticCliService] = None,
) -> int:
    """Run the CLI and return a process-style exit code."""

    parser = build_parser()
    output = out if out is not None else sys.stdout
    normalized_argv = list(argv) if argv is not None else None
    if normalized_argv and _looks_like_program_name(normalized_argv[0], parser.prog):
        normalized_argv = normalized_argv[1:]
    args = None
    with contextlib.redirect_stderr(output), contextlib.redirect_stdout(output):
        try:
            args = parser.parse_args(normalized_argv)
        except SystemExit as exc:
            code = exc.code if isinstance(exc.code, int) else 2
            return code

    if args.command == "run":
        active_service = service if service is not None else AgenticCliService()
        try:
            receipt = active_service.run(RunCommand(prompt=args.prompt, session_id=args.session_id))
        except AgenticCliError as exc:
            print(f"error: {exc}", file=output)
            return 2

        print(format_run_receipt(receipt), file=output)
        return 0

    if args.command == "replay-sla":
        try:
            result = run_replay_sla(
                total=args.total,
                batch_size=args.batch_size,
                sla_ms=args.sla_ms,
                now_ms=now_ms,
            )
        except ValueError as exc:
            print(f"error: {exc}", file=output)
            return 2

        print(format_replay_sla_result(result), file=output)
        return 0 if result.within_sla else 1

    if args.command == "tui":
        from .tui import run_tui

        active_service = service if service is not None else AgenticCliService()
        run_tui(service=active_service)
        return 0

    parser.print_help(file=output)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
