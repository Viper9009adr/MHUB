"""Shared fork context storage.

Provides a simple in-memory dict for passing fork-related context
between components within the same process.  This avoids async
task-scoping issues entirely — fork_id is threaded through all
calls as an explicit parameter.
"""

from __future__ import annotations

_contexts: dict[str, dict[str, str]] = {}


def set_context(fork_id: str, ctx: dict[str, str]) -> None:
    """Store context data for a given fork_id.

    Args:
        fork_id: Unique identifier for the fork scenario.
        ctx: Dictionary of context key-value pairs (string to string).
    """
    _contexts[fork_id] = ctx


def get_context(fork_id: str) -> dict[str, str] | None:
    """Retrieve context data for a given fork_id.

    Args:
        fork_id: Unique identifier for the fork scenario.

    Returns:
        The context dict if found, otherwise None.
    """
    return _contexts.get(fork_id)


def clear_context(fork_id: str) -> None:
    """Remove context data for a given fork_id.

    Args:
        fork_id: Unique identifier for the fork scenario.
    """
    _contexts.pop(fork_id, None)


__all__ = ["set_context", "get_context", "clear_context"]
