"""FastAPI application factory for the Meridian HUB API."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from env import resolve_config
from src.agentic_cli.config import (
    DEFAULT_DATABASE_URL,
    DEFAULT_DB_POOL_SIZE,
    DEFAULT_GRPC_PORT,
    DEFAULT_LLM_MODEL,
    DEFAULT_LLM_PROVIDER,
    DEFAULT_REDIS_ENABLED,
    DEFAULT_REDIS_URL,
)
from src.api.routes.run import create_run_router
from src.api.schemas.run import ErrorResponse, HealthResponse
from src.llm.factory import create_provider
from src.llm.provider import LLMProvider
from src.orc.daemon import OrcDaemon
from src.orc.dispatch import OrcDispatch

APP_GRPC_PORT_DEFAULT = DEFAULT_GRPC_PORT
APP_API_PORT_DEFAULT = 8123


@asynccontextmanager
async def lifespan(
    app: FastAPI,
) -> AsyncIterator[dict]:
    """Manage ORC daemon lifecycle via FastAPI lifespan events."""
    load_dotenv(Path(__file__).parent.parent.parent / ".env")
    llm_provider: LLMProvider | None = None
    try:
        llm_provider = create_provider(
            provider_name=app.state.llm_provider,
            api_key=app.state.llm_api_key,
            model=app.state.llm_model,
        )
    except Exception as exc:
        # Non-fatal: daemon starts without LLM if provider init fails
        import logging

        logging.getLogger(__name__).warning("LLM provider init failed: %s", exc)

    daemon = OrcDaemon(
        grpc_port=app.state.grpc_port,
        database_url=app.state.database_url,
        db_pool_size=app.state.db_pool_size,
        llm_provider=llm_provider,
        llm_model=app.state.llm_model,
        redis_url=app.state.redis_url,
        redis_enabled=app.state.redis_enabled,
    )
    await daemon.start()
    try:
        yield {"daemon": daemon, "llm_provider": llm_provider}
    finally:
        await daemon.stop()


def create_app(
    dispatch: OrcDispatch | None = None,
    grpc_port: int = APP_GRPC_PORT_DEFAULT,
    database_url: str = DEFAULT_DATABASE_URL,
    db_pool_size: int = DEFAULT_DB_POOL_SIZE,
    api_port: int | None = None,
    llm_provider: str | None = None,
    llm_model: str | None = None,
    llm_api_key: str | None = None,
    redis_url: str | None = None,
    redis_enabled: bool | None = None,
) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        dispatch: Optional OrcDispatch instance. A default is created if None.
        grpc_port: Port for the gRPC server.
        database_url: PostgreSQL connection string.
        db_pool_size: Number of connections in the pool.
        api_port: HTTP API port (informational, used by dev.sh).
        llm_provider: LLM provider name (openai, anthropic, nvidia, openrouter).
        llm_model: Model identifier.
        llm_api_key: API key for the LLM provider.
        redis_url: Redis connection URL.
        redis_enabled: Whether to enable Redis pub/sub.

    Returns:
        Configured FastAPI application with routes and error handlers.
    """
    # Resolve config via CLI > ENV > .env > default chain
    resolved_api_port = resolve_config("api_port", cli_val=api_port)
    resolved_llm_provider = resolve_config("llm_provider", cli_val=llm_provider)
    resolved_llm_model = resolve_config("llm_model", cli_val=llm_model)
    resolved_redis_url = resolve_config("redis_url", cli_val=redis_url)
    resolved_redis_enabled = resolve_config("redis_enabled", cli_val=redis_enabled)

    app = FastAPI(
        title="Meridian HUB API",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.state.grpc_port = grpc_port
    app.state.database_url = database_url
    app.state.db_pool_size = db_pool_size
    app.state.api_port = resolved_api_port
    app.state.llm_provider = resolved_llm_provider
    app.state.llm_model = resolved_llm_model
    app.state.llm_api_key = llm_api_key
    app.state.redis_url = resolved_redis_url
    app.state.redis_enabled = resolved_redis_enabled

    orc = dispatch or OrcDispatch()

    # Register routes
    app.include_router(create_run_router(orc))

    # Health endpoint
    @app.get("/health", response_model=HealthResponse, tags=["health"])
    async def health() -> HealthResponse:
        """Operational readiness probe."""
        return HealthResponse()

    # Error handlers
    @app.exception_handler(ValueError)
    async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content=ErrorResponse(detail=str(exc)).model_dump(),
        )

    @app.exception_handler(Exception)
    async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(detail="Internal server error").model_dump(),
        )

    return app


__all__ = ["create_app"]
