"""Graceful shutdown handler for the agentic runtime.

Manages signal handling and coordinated shutdown of all runtime
components.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any, Callable

logger = logging.getLogger(__name__)


class ShutdownHandler:
    """Handler for graceful shutdown of the agentic runtime.

    Registers signal handlers and coordinates shutdown of all
    components in the correct order.

    Args:
        shutdown_callback: Async callback to invoke on shutdown.
    """

    def __init__(
        self,
        shutdown_callback: Callable[[], Any] | None = None,
    ) -> None:
        self._shutdown_callback = shutdown_callback
        self._shutdown_event = asyncio.Event()
        self._registered = False

    def register_signals(self) -> None:
        """Register signal handlers for graceful shutdown.

        Handles SIGTERM and SIGINT for clean shutdown.
        """
        if self._registered:
            return

        loop = asyncio.get_event_loop()

        def handle_signal(sig: signal.Signals) -> None:
            logger.info("Received signal %s, initiating shutdown", sig.name)
            self._shutdown_event.set()
            if self._shutdown_callback is not None:
                asyncio.create_task(self._shutdown_callback())

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, handle_signal, sig)
            except NotImplementedError:
                # Windows doesn't support add_signal_handler
                signal.signal(sig, lambda s, f: handle_signal(s))

        self._registered = True
        logger.debug("Shutdown signal handlers registered")

    def unregister_signals(self) -> None:
        """Unregister signal handlers."""
        loop = asyncio.get_event_loop()

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.remove_signal_handler(sig)
            except (NotImplementedError, ValueError):
                pass

        self._registered = False
        logger.debug("Shutdown signal handlers unregistered")

    async def wait_for_shutdown(self) -> None:
        """Wait for shutdown signal.

        Blocks until a shutdown signal is received.
        """
        self.register_signals()
        await self._shutdown_event.wait()

    @property
    def is_shutdown_requested(self) -> bool:
        """Check if shutdown has been requested."""
        return self._shutdown_event.is_set()

    def request_shutdown(self) -> None:
        """Manually request shutdown."""
        logger.info("Shutdown manually requested")
        self._shutdown_event.set()


async def graceful_shutdown(
    components: dict[str, Any],
    timeout: float = 30.0,
) -> None:
    """Perform graceful shutdown of multiple components.

    Shuts down components in reverse order of registration,
    with timeout for each component.

    Args:
        components: Dict of name -> component with close/shutdown method.
        timeout: Maximum time for each component shutdown.
    """
    logger.info("Starting graceful shutdown of %d components", len(components))

    # Shutdown in reverse order
    names = list(components.keys())[::-1]
    for name in names:
        component = components[name]
        try:
            if hasattr(component, "shutdown"):
                await asyncio.wait_for(component.shutdown(), timeout=timeout)
            elif hasattr(component, "close"):
                await asyncio.wait_for(component.close(), timeout=timeout)
            logger.info("Shutdown complete: %s", name)
        except asyncio.TimeoutError:
            logger.warning("Shutdown timeout for component: %s", name)
        except Exception as exc:
            logger.error("Shutdown error for %s: %s", name, exc)

    logger.info("Graceful shutdown complete")


__all__ = ["ShutdownHandler", "graceful_shutdown"]
