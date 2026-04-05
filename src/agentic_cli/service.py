"""Service layer for the Phase 1 local AGENTIC CLI backend core."""

from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator

from .config import AgenticCliConfig
from .contracts import RunCommand, RunReceipt
from .domain import BackendReply, ConversationTurn
from .memory import InMemoryConversationStore

try:
    from src.llm.models import LLMChunk
    from src.llm.errors import LLMRateLimitError, LLMTimeoutError
except ImportError:
    LLMChunk = None  # type: ignore[assignment,misc]
    LLMRateLimitError = Exception  # type: ignore[assignment,misc]
    LLMTimeoutError = Exception  # type: ignore[assignment,misc]


class AgenticCliService:
    """Runs validated commands against the current in-process backend implementation."""

    def __init__(
        self,
        memory: InMemoryConversationStore | None = None,
        config: AgenticCliConfig | None = None,
    ) -> None:
        """Create a service with optional injected memory store and config."""

        self._memory = memory if memory is not None else InMemoryConversationStore()
        self._config = config if config is not None else AgenticCliConfig()
        self._last_session_id: str | None = None
        self._last_turn_index: int | None = None

    def run(self, command: RunCommand) -> RunReceipt:
        """Append a prompt to session history and return a local backend receipt."""

        session_id = command.session_id or self._memory.allocate_session_id(self._config.session_prefix)
        turn = self._memory.append_turn(session_id, command.prompt)
        reply = self._run_local_backend(turn)
        return RunReceipt(
            session_id=reply.session_id,
            turn_index=reply.turn_index,
            backend=reply.backend,
            output=reply.output,
        )

    async def async_run(self, command: RunCommand) -> RunReceipt:
        """Async variant of run for use in async dispatch pipelines.

        Delegates to the synchronous run implementation since the Phase 1
        local backend is CPU-bound and deterministic.
        """
        return self.run(command)

    async def async_run_with_llm(
        self,
        command: RunCommand,
        llm_provider: Any,
        llm_model: str | None = None,
    ) -> RunReceipt:
        """Run a command using an LLM provider instead of the local backend.

        Args:
            command: The validated run command.
            llm_provider: An LLMProvider instance (from src.llm.factory).
            llm_model: Model override. Falls back to config default.

        Returns:
            RunReceipt with the LLM response as output.
        """
        session_id = command.session_id or self._memory.allocate_session_id(self._config.session_prefix)
        turn = self._memory.append_turn(session_id, command.prompt)

        model = llm_model or self._config.llm_model
        response = await llm_provider.complete(prompt=command.prompt, model=model)

        return RunReceipt(
            session_id=session_id,
            turn_index=turn.turn_index,
            backend=f"llm:{model}",
            output=response.text,
        )

    async def async_stream_with_llm(
        self,
        command: RunCommand,
        llm_provider: Any,
        llm_model: str | None = None,
    ) -> AsyncIterator[str]:
        """Stream an LLM response token-by-token with up to 3 retry attempts.

        Retries on LLMRateLimitError and LLMTimeoutError with a 1-second backoff.
        All other exceptions propagate immediately.

        Args:
            command: The validated run command.
            llm_provider: An LLMProvider instance that exposes ``stream()``.
            llm_model: Model override. Falls back to config default.

        Yields:
            Text fragments (str) as they arrive from the provider.
        """
        model = llm_model or self._config.llm_model
        session_id = command.session_id or self._memory.allocate_session_id(self._config.session_prefix)
        turn = self._memory.append_turn(session_id, command.prompt)

        _retryable = (LLMRateLimitError, LLMTimeoutError)
        last_exc: BaseException | None = None

        for attempt in range(3):
            try:
                async for chunk in llm_provider.stream(prompt=command.prompt, model=model):
                    if LLMChunk is not None and isinstance(chunk, LLMChunk):
                        yield chunk.text
                    else:
                        yield str(chunk)
                self._last_session_id = session_id
                self._last_turn_index = turn.turn_index
                return
            except _retryable as exc:  # type: ignore[misc]
                last_exc = exc
                if attempt < 2:
                    await asyncio.sleep(1)
            except Exception:
                raise

        raise last_exc  # type: ignore[misc]

    def _run_local_backend(self, turn: ConversationTurn) -> BackendReply:
        """Produce the deterministic Phase 1 local backend response string."""

        prompt = turn.prompt
        output = f"local:{turn.turn_index}:{prompt}"
        return BackendReply(
            session_id=turn.session_id,
            turn_index=turn.turn_index,
            backend=self._config.backend_mode,
            output=output,
        )


__all__ = ["AgenticCliService"]
