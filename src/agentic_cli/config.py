"""Static configuration defaults for the local AGENTIC CLI slice."""

from __future__ import annotations

from dataclasses import dataclass, field


DEFAULT_BACKEND_MODE = "local"
DEFAULT_PROGRAM_NAME = "meridian-hub-agentic-cli"
DEFAULT_SESSION_PREFIX = "session"
DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/meridian"
DEFAULT_GRPC_PORT = 50052
DEFAULT_DB_POOL_SIZE = 5
DEFAULT_LLM_PROVIDER = "openai"
DEFAULT_LLM_MODEL = "gpt-4o"
DEFAULT_API_PORT = 8123
DEFAULT_REDIS_URL = "redis://localhost:6379/0"
DEFAULT_REDIS_ENABLED = False


@dataclass(frozen=True)
class AgenticCliConfig:
    """Holds code-level defaults for the current in-process backend."""

    backend_mode: str = DEFAULT_BACKEND_MODE
    program_name: str = DEFAULT_PROGRAM_NAME
    session_prefix: str = DEFAULT_SESSION_PREFIX
    database_url: str = DEFAULT_DATABASE_URL
    grpc_port: int = DEFAULT_GRPC_PORT
    db_pool_size: int = DEFAULT_DB_POOL_SIZE
    db_credentials: dict[str, str] = field(default_factory=dict)
    llm_provider: str = DEFAULT_LLM_PROVIDER
    llm_model: str = DEFAULT_LLM_MODEL
    api_port: int = DEFAULT_API_PORT
    redis_url: str = DEFAULT_REDIS_URL
    redis_enabled: bool = DEFAULT_REDIS_ENABLED


__all__ = [
    "AgenticCliConfig",
    "DEFAULT_BACKEND_MODE",
    "DEFAULT_PROGRAM_NAME",
    "DEFAULT_SESSION_PREFIX",
    "DEFAULT_DATABASE_URL",
    "DEFAULT_GRPC_PORT",
    "DEFAULT_DB_POOL_SIZE",
    "DEFAULT_LLM_PROVIDER",
    "DEFAULT_LLM_MODEL",
    "DEFAULT_API_PORT",
    "DEFAULT_REDIS_URL",
    "DEFAULT_REDIS_ENABLED",
]
