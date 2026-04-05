"""Domain records used by the local in-memory AGENTIC CLI backend."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ConversationTurn:
    """Stores one prompt recorded for a session in turn order."""

    session_id: str
    turn_index: int
    prompt: str


@dataclass(frozen=True)
class BackendReply:
    """Carries the backend response that the service turns into a receipt."""

    session_id: str
    turn_index: int
    backend: str
    output: str


__all__ = ["BackendReply", "ConversationTurn"]
