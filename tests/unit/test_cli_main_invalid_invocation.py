from __future__ import annotations

from io import StringIO

from src.agentic_cli.cli import main


def test_main_returns_two_for_unknown_subcommand() -> None:
    out = StringIO()

    code = main(["unknown-command"], out=out)

    assert code == 2
    assert "invalid choice" in out.getvalue()


def test_main_accepts_argv_prefixed_with_program_name() -> None:
    out = StringIO()

    code = main(["/usr/local/bin/meridian-hub-agentic-cli", "--help"], out=out)

    assert code == 0
    assert "usage:" in out.getvalue()


def test_main_accepts_windows_style_prefixed_program_name() -> None:
    out = StringIO()

    code = main([r"C:\\Tools\\meridian-hub-agentic-cli", "--help"], out=out)

    assert code == 0
    assert "usage:" in out.getvalue()


def test_main_accepts_windows_exe_prefixed_program_name_case_insensitive() -> None:
    out = StringIO()

    code = main([r"C:\\Tools\\Meridian-Hub-Agentic-CLI.EXE", "--help"], out=out)

    assert code == 0
    assert "usage:" in out.getvalue()
