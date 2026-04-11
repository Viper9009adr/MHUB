"""ORC daemon for managing gRPC server lifecycle via FastAPI lifespan events.

No raw signal handlers are used. All lifecycle management is handled
through FastAPI lifespan context managers. Includes graceful shutdown
support with configurable grace periods.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from contextlib import asynccontextmanager
from typing import Any

import grpc

from src.hub.hub_pb2_grpc import add_HubServiceServicer_to_server
from src.hub.service import HubService
from src.llm.provider import LLMProvider
from src.orc.fork_detector import ForkDetector
from src.orc.fork_handler import ForkHandler
from src.orc.hal_agent import HalAgent
from src.orc.redis_bridge import RedisBridge
from src.storage.null_backend import NullStorageBackend
from src.storage.pg import StorageBackend, init_storage
from src.storage.retry import RetryConfig

logger = logging.getLogger(__name__)

GRPC_PORT_DEFAULT = 50052
DATABASE_URL_DEFAULT = "postgresql://postgres:postgres@localhost:5432/meridian"
DB_POOL_SIZE_DEFAULT = 5
REDIS_URL_DEFAULT = "redis://localhost:6379/0"
GRACEFUL_SHUTDOWN_TIMEOUT = 30.0
GRPC_GRACE_PERIOD = 0


def _env_bool(key: str, default: bool = False) -> bool:
    """Read a boolean-ish environment variable."""
    val = os.environ.get(key)
    if val is None:
        return default
    val = val.lower()
    if val in ("1", "true", "yes"):
        return True
    if val in ("0", "false", "no"):
        return False
    return default


class OrcDaemon:
    """Manages the gRPC server and storage backend lifecycle.

    Designed to be integrated with FastAPI lifespan events.
    Supports both real PostgreSQL and null (in-memory) backends.

    The STORAGE_ENABLED env var (or constructor flag) controls whether
    the storage backend is passed to HubService. When disabled the
    service falls back to its original in-memory dicts.

    Supports graceful shutdown with configurable grace periods for
    each component.
    """

    def __init__(
        self,
        grpc_port: int = GRPC_PORT_DEFAULT,
        database_url: str = DATABASE_URL_DEFAULT,
        db_pool_size: int = DB_POOL_SIZE_DEFAULT,
        use_null_backend: bool = False,
        storage_enabled: bool | None = None,
        retry_config: RetryConfig | None = None,
        redis_url: str = REDIS_URL_DEFAULT,
        redis_enabled: bool = True,
        llm_provider: LLMProvider | None = None,
        llm_model: str = "Claudito",
        grpc_grace_period: int = GRPC_GRACE_PERIOD,
        shutdown_timeout: float = GRACEFUL_SHUTDOWN_TIMEOUT,
        debate_rounds: int = 3,
    ) -> None:
        self._grpc_port = grpc_port
        self._database_url = database_url
        self._db_pool_size = db_pool_size
        self._use_null_backend = use_null_backend or _env_bool("MERIDIAN_NULL_BACKEND")
        self._storage_enabled = (
            storage_enabled
            if storage_enabled is not None
            else _env_bool("STORAGE_ENABLED", default=True)
        )
        self._retry_config = retry_config
        self._redis_url = redis_url
        self._redis_enabled = redis_enabled and _env_bool("REDIS_ENABLED", default=True)
        self._llm_provider = llm_provider
        self._llm_model = llm_model
        self._grpc_grace_period = grpc_grace_period
        self._shutdown_timeout = shutdown_timeout
        self._debate_rounds = int(os.environ.get("MERIDIAN_DEBATE_ROUNDS", str(debate_rounds)))
        self._server: grpc.aio.Server | None = None
        self._storage: StorageBackend | NullStorageBackend | None = None
        self._hub_service: HubService | None = None
        self._redis: RedisBridge | None = None
        self._fork_handler: ForkHandler | None = None
        self._fork_detector: ForkDetector | None = None
        self._fork_listener_task: asyncio.Task | None = None
        self._hal_agent: HalAgent | None = None
        self._shutdown_event = asyncio.Event()
        self._signal_handlers_registered = False

    async def start(self) -> None:
        """Start the gRPC server and initialise storage."""
        logger.info("ORC daemon starting on port %d", self._grpc_port)

        # Initialise storage backend
        if self._storage_enabled:
            if self._use_null_backend:
                logger.info("Using null (in-memory) storage backend")
                self._storage = NullStorageBackend()
            else:
                self._storage = await init_storage(
                    database_url=self._database_url,
                    pool_size=self._db_pool_size,
                )
            logger.info("Storage backend initialised")
        else:
            logger.info("Storage disabled — HubService will use in-memory dicts")

        # Initialise Redis bridge
        if self._redis_enabled:
            self._redis = RedisBridge(url=self._redis_url)
            await self._redis.connect()
            logger.info("Redis bridge initialised")

            # Create ForkHandler and ForkDetector
            if self._storage is not None:
                self._fork_handler = ForkHandler(
                    storage=self._storage,
                    redis=self._redis,
                )
                self._fork_detector = ForkDetector(
                    redis=self._redis,
                    handler=self._fork_handler,
                )
                # Start listening for divergence events
                self._fork_listener_task = self._fork_detector.listen_for_divergence()
                logger.info("ForkDetector started")

                # Pending-fork recovery: re-publish any pending forks
                await self._recover_pending_forks()

            # Instantiate HAL agent when Redis is enabled and LLM provider available
            if self._llm_provider is not None:
                self._hal_agent = HalAgent(
                    llm_provider=self._llm_provider,
                    llm_model=self._llm_model,
                    redis=self._redis,
                )
                logger.info("HAL agent initialised (model=%s)", self._llm_model)

        # Start gRPC server — pass storage, fork_handler, and llm_provider
        self._hub_service = HubService(
            storage=self._storage if self._storage_enabled else None,
            fork_handler=self._fork_handler,
            llm_provider=self._llm_provider,
            llm_model=self._llm_model,
            debate_rounds=self._debate_rounds,
        )
        self._server = grpc.aio.server()
        add_HubServiceServicer_to_server(self._hub_service, self._server)
        self._server.add_insecure_port(f"[::]:{self._grpc_port}")
        await self._server.start()
        logger.info("gRPC server started on port %d", self._grpc_port)

    async def _recover_pending_forks(self) -> None:
        """Re-publish pending forks on startup so notifications are not lost."""
        if self._fork_handler is None or self._redis is None:
            return
        try:
            pending = await self._storage.get_pending_forks()  # type: ignore[union-attr]
            for record in pending:
                await self._fork_handler.publish_pending(record.fork_id, record)
            logger.info("Recovered %d pending fork(s)", len(pending))
        except Exception as exc:
            logger.warning("Pending fork recovery failed: %s", exc)

    async def stop(self) -> None:
        """Gracefully stop the gRPC server and close storage.

        Shutdown order:
        1. Stop fork detector (stops accepting new divergence events)
        2. Stop gRPC server with grace period
        3. Close storage backend
        4. Disconnect Redis

        Each step has a timeout to prevent hanging.
        """
        logger.info("ORC daemon stopping")

        # Stop fork detector listener first
        if self._fork_detector is not None:
            try:
                await asyncio.wait_for(
                    self._fork_detector.stop(),
                    timeout=self._shutdown_timeout / 4,
                )
            except asyncio.TimeoutError:
                logger.warning("ForkDetector stop timed out")
            except Exception as exc:
                logger.warning("ForkDetector stop error: %s", exc)

        # Stop gRPC server with grace period
        if self._server is not None:
            try:
                await asyncio.wait_for(
                    self._server.stop(grace=self._grpc_grace_period),
                    timeout=self._shutdown_timeout / 2,
                )
                logger.info("gRPC server stopped")
            except asyncio.TimeoutError:
                logger.warning("gRPC server stop timed out")
            except Exception as exc:
                logger.warning("gRPC server stop error: %s", exc)

        # Close storage backend
        if self._storage is not None:
            try:
                await asyncio.wait_for(
                    self._storage.close(),
                    timeout=self._shutdown_timeout / 4,
                )
                logger.info("Storage backend closed")
            except asyncio.TimeoutError:
                logger.warning("Storage close timed out")
            except Exception as exc:
                logger.warning("Storage close error: %s", exc)

        # Disconnect Redis
        if self._redis is not None:
            try:
                await asyncio.wait_for(
                    self._redis.disconnect(),
                    timeout=self._shutdown_timeout / 4,
                )
                logger.info("Redis bridge disconnected")
            except asyncio.TimeoutError:
                logger.warning("Redis disconnect timed out")
            except Exception as exc:
                logger.warning("Redis disconnect error: %s", exc)

        logger.info("ORC daemon shutdown complete")

    def register_shutdown_signals(self) -> None:
        """Register signal handlers for graceful shutdown.

        Handles SIGTERM and SIGINT for clean shutdown.
        """
        if self._signal_handlers_registered:
            return

        loop = asyncio.get_event_loop()

        def handle_signal(sig: signal.Signals) -> None:
            logger.info("Received signal %s, initiating shutdown", sig.name)
            self._shutdown_event.set()
            # Trigger stop in a task
            asyncio.create_task(self.stop())

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, handle_signal, sig)
            except NotImplementedError:
                # Windows doesn't support add_signal_handler
                signal.signal(sig, lambda s, f: handle_signal(s))

        self._signal_handlers_registered = True
        logger.debug("Shutdown signal handlers registered")

    async def wait_for_shutdown(self) -> None:
        """Wait for shutdown signal.

        Blocks until a shutdown signal is received.
        """
        self.register_shutdown_signals()
        await self._shutdown_event.wait()

    @property
    def is_shutdown_requested(self) -> bool:
        """Check if shutdown has been requested."""
        return self._shutdown_event.is_set()

    @property
    def hub_service(self) -> HubService | None:
        """Access the hub service instance."""
        return self._hub_service

    @property
    def storage(self) -> StorageBackend | NullStorageBackend | None:
        """Access the storage backend instance."""
        return self._storage

    @property
    def grpc_port(self) -> int:
        """The port the gRPC server is bound to."""
        return self._grpc_port

    @property
    def storage_enabled(self) -> bool:
        """Whether storage is enabled."""
        return self._storage_enabled

    @property
    def fork_handler(self) -> ForkHandler | None:
        """Access the fork handler instance."""
        return self._fork_handler

    @property
    def fork_detector(self) -> ForkDetector | None:
        """Access the fork detector instance."""
        return self._fork_detector

    @property
    def redis(self) -> RedisBridge | None:
        """Access the Redis bridge instance."""
        return self._redis

    @property
    def hal_agent(self) -> HalAgent | None:
        """Access the HAL agent instance."""
        return self._hal_agent

    @property
    def llm_provider(self) -> LLMProvider | None:
        """Access the LLM provider instance."""
        return self._llm_provider


@asynccontextmanager
async def orc_daemon_lifespan(
    grpc_port: int = GRPC_PORT_DEFAULT,
    database_url: str = DATABASE_URL_DEFAULT,
    db_pool_size: int = DB_POOL_SIZE_DEFAULT,
):
    """FastAPI lifespan context manager for the ORC daemon.

    Usage:
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            async with orc_daemon_lifespan(...) as daemon:
                yield
    """
    daemon = OrcDaemon(
        grpc_port=grpc_port,
        database_url=database_url,
        db_pool_size=db_pool_size,
    )
    await daemon.start()
    try:
        yield daemon
    finally:
        await daemon.stop()
