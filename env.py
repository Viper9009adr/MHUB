"""Environment-backed configuration helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping
import os

from src.agentic_cli.config import (
    DEFAULT_API_PORT,
    DEFAULT_GRPC_PORT,
    DEFAULT_LLM_MODEL,
    DEFAULT_LLM_PROVIDER,
    DEFAULT_REDIS_ENABLED,
    DEFAULT_REDIS_URL,
)

_GRPC_PORT_KEY = "MERIDIAN_GRPC_PORT"

_CONFIG_ENV_KEYS: dict[str, str] = {
    "grpc_port": "MERIDIAN_GRPC_PORT",
    "llm_provider": "LLM_PROVIDER",
    "llm_model": "LLM_MODEL",
    "api_port": "API_PORT",
    "redis_url": "REDIS_URL",
    "redis_enabled": "REDIS_ENABLED",
}

_CONFIG_DEFAULTS: dict[str, Any] = {
    "grpc_port": DEFAULT_GRPC_PORT,
    "llm_provider": DEFAULT_LLM_PROVIDER,
    "llm_model": DEFAULT_LLM_MODEL,
    "api_port": DEFAULT_API_PORT,
    "redis_url": DEFAULT_REDIS_URL,
    "redis_enabled": DEFAULT_REDIS_ENABLED,
}

_DOTENV_PATH: Path = Path(__file__).parent / ".env"


def _read_dotenv_var(dotenv_path: Path, key: str) -> str | None:
    if not dotenv_path.exists():
        return None
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        env_key, env_value = line.split("=", maxsplit=1)
        if env_key.strip() != key:
            continue
        value = env_value.strip().strip("\"'")
        if value:
            return value
    return None


def resolve_config(
    key: str,
    cli_val: Any = None,
    env_key: str | None = None,
    dotenv_path: Path | None = None,
    default: Any = None,
) -> Any:
    """Resolve a configuration value with precedence: CLI > ENV > .env > default.

    Args:
        key: The configuration key (e.g. "grpc_port", "llm_provider").
        cli_val: Value passed explicitly from CLI. Takes highest precedence.
        env_key: Override the environment variable name. Defaults to lookup in
            _CONFIG_ENV_KEYS.
        dotenv_path: Path to the .env file. Defaults to project-root .env.
        default: Fallback value if nothing else is set.

    Returns:
        The resolved configuration value.
    """
    # 1. CLI value
    if cli_val is not None:
        return cli_val

    # 2. Environment variable
    effective_env_key = env_key or _CONFIG_ENV_KEYS.get(key)
    if effective_env_key:
        env_value = os.environ.get(effective_env_key)
        if env_value is not None:
            return _coerce_value(key, env_value)

    # 3. .env file
    path = dotenv_path or _DOTENV_PATH
    if effective_env_key:
        dotenv_value = _read_dotenv_var(path, effective_env_key)
        if dotenv_value is not None:
            return _coerce_value(key, dotenv_value)

    # 4. Default
    if default is not None:
        return default
    return _CONFIG_DEFAULTS.get(key)


def _coerce_value(key: str, raw: str) -> Any:
    """Coerce a string value to the appropriate Python type for the key."""
    if key in ("grpc_port", "api_port"):
        return int(raw)
    if key == "redis_enabled":
        return raw.lower() in ("1", "true", "yes")
    return raw


def resolve_grpc_port(
    cli_port: int | None = None,
    env: Mapping[str, str] | None = None,
    dotenv_path: Path | None = None,
) -> int:
    """Resolve gRPC port with precedence CLI > ENV > .env > default.

    Deprecated: use resolve_config("grpc_port", cli_val=cli_port) instead.
    """
    if cli_port is not None:
        return cli_port

    environment = os.environ if env is None else env
    env_value = environment.get(_GRPC_PORT_KEY)
    if env_value:
        return int(env_value)

    path = dotenv_path or _DOTENV_PATH
    dotenv_value = _read_dotenv_var(path, _GRPC_PORT_KEY)
    if dotenv_value:
        return int(dotenv_value)

    return DEFAULT_GRPC_PORT


__all__ = ["resolve_config", "resolve_grpc_port"]
