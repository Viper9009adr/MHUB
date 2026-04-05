from __future__ import annotations

from collections.abc import Callable, Sequence
from io import StringIO

from src.agentic_cli.cli import main


def test_replay_sla_cli_returns_zero_within_sla(make_clock: Callable[[Sequence[int]], Callable[[], int]]) -> None:
    out = StringIO()

    code = main(
        ["replay-sla", "--total", "5000", "--batch-size", "250", "--sla-ms", "2000"],
        out=out,
        now_ms=make_clock([100, 1800]),
    )

    assert code == 0
    assert out.getvalue().strip() == "seen=5000 elapsed_ms=1700 within_sla=true"


def test_replay_sla_cli_returns_one_for_sla_breach(make_clock: Callable[[Sequence[int]], Callable[[], int]]) -> None:
    out = StringIO()

    code = main(
        ["replay-sla", "--total", "5000", "--batch-size", "250", "--sla-ms", "2000"],
        out=out,
        now_ms=make_clock([100, 2301]),
    )

    assert code == 1
    assert out.getvalue().strip() == "seen=5000 elapsed_ms=2201 within_sla=false"


def test_replay_sla_cli_returns_two_for_invalid_args() -> None:
    out = StringIO()

    code = main(
        ["replay-sla", "--total", "-1", "--batch-size", "250", "--sla-ms", "2000"],
        out=out,
    )

    assert code == 2
    assert out.getvalue().strip() == "error: total must be >= 0"


def test_replay_sla_cli_handles_parse_errors_without_exiting() -> None:
    out = StringIO()

    code = main(
        ["replay-sla", "--total", "not-a-number", "--batch-size", "250", "--sla-ms", "2000"],
        out=out,
    )

    assert code == 2
    assert "invalid int value" in out.getvalue()


def test_replay_sla_cli_requires_subcommand() -> None:
    out = StringIO()

    code = main([], out=out)

    assert code == 2
    assert "the following arguments are required: command" in out.getvalue()


def test_replay_sla_cli_help_returns_zero() -> None:
    out = StringIO()

    code = main(["--help"], out=out)

    assert code == 0
    assert "usage:" in out.getvalue()


def test_replay_sla_cli_help_returns_zero_with_program_name_prefix() -> None:
    out = StringIO()

    code = main(["/usr/local/bin/meridian-hub-agentic-cli", "--help"], out=out)

    assert code == 0
    assert "usage:" in out.getvalue()


def test_replay_sla_cli_accepts_windows_style_program_name_prefix(
    make_clock: Callable[[Sequence[int]], Callable[[], int]],
) -> None:
    out = StringIO()

    code = main(
        [
            r"C:\\Tools\\meridian-hub-agentic-cli",
            "replay-sla",
            "--total",
            "1",
            "--batch-size",
            "1",
            "--sla-ms",
            "2000",
        ],
        out=out,
        now_ms=make_clock([100, 101]),
    )

    assert code == 0
    assert out.getvalue().strip() == "seen=1 elapsed_ms=1 within_sla=true"


def test_replay_sla_cli_accepts_windows_exe_program_name_prefix_case_insensitive(
    make_clock: Callable[[Sequence[int]], Callable[[], int]],
) -> None:
    out = StringIO()

    code = main(
        [
            r"C:\\Tools\\Meridian-Hub-Agentic-CLI.EXE",
            "replay-sla",
            "--total",
            "1",
            "--batch-size",
            "1",
            "--sla-ms",
            "2000",
        ],
        out=out,
        now_ms=make_clock([100, 101]),
    )

    assert code == 0
    assert out.getvalue().strip() == "seen=1 elapsed_ms=1 within_sla=true"
