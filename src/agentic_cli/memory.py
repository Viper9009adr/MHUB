"""In-memory conversation persistence for the current AGENTIC CLI slice."""

from __future__ import annotations

from .domain import ConversationTurn


class InMemoryConversationStore:
    """Tracks session turns in process and allocates sequential session ids."""

    def __init__(self) -> None:
        """Initialize empty session history and the next numeric session counter."""

        self._sessions: dict[str, list[ConversationTurn]] = {}
        self._next_session = 1

    def allocate_session_id(self, prefix: str) -> str:
        """Return the next session id using the provided prefix."""

        session_id = f"{prefix}-{self._next_session}"
        self._next_session += 1
        return session_id

    def append_turn(self, session_id: str, prompt: str) -> ConversationTurn:
        """Append a prompt to a session and return the stored turn record."""

        turns = self._sessions.setdefault(session_id, [])
        turn = ConversationTurn(session_id=session_id, turn_index=len(turns) + 1, prompt=prompt)
        turns.append(turn)
        return turn

    def read_history(self, session_id: str) -> tuple[ConversationTurn, ...]:
        """Return an immutable snapshot of the stored turns for one session."""

        return tuple(self._sessions.get(session_id, ()))


__all__ = ["InMemoryConversationStore"]
