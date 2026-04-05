"""Unit tests for environment configuration loader."""

from __future__ import annotations

from pathlib import Path

from env import resolve_grpc_port
from src.agentic_cli.config import DEFAULT_GRPC_PORT


def test_cli_precedence_over_env_and_dotenv(tmp_path: Path) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text("MERIDIAN_GRPC_PORT=50054\n", encoding="utf-8")
    port = resolve_grpc_port(
        cli_port=50055,
        env={"MERIDIAN_GRPC_PORT": "50053"},
        dotenv_path=dotenv,
    )
    assert port == 50055


def test_env_precedence_over_dotenv(tmp_path: Path) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text("MERIDIAN_GRPC_PORT=50054\n", encoding="utf-8")
    port = resolve_grpc_port(env={"MERIDIAN_GRPC_PORT": "50053"}, dotenv_path=dotenv)
    assert port == 50053


def test_dotenv_precedence_over_default(tmp_path: Path) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text("MERIDIAN_GRPC_PORT=50054\n", encoding="utf-8")
    port = resolve_grpc_port(env={}, dotenv_path=dotenv)
    assert port == 50054


def test_default_when_cli_env_and_dotenv_absent(tmp_path: Path) -> None:
    port = resolve_grpc_port(env={}, dotenv_path=tmp_path / ".env")
    assert port == DEFAULT_GRPC_PORT
