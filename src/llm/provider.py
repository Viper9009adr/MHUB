"""LLM provider protocol and typed errors."""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator
from typing import Protocol

from src.llm.models import LLMChunk, LLMResponse


class LLMProvider(Protocol):
    """Protocol that all LLM provider implementations must satisfy."""

    async def complete(
        self,
        prompt: str,
        model: str,
        *,
        messages: list[dict] | None = None,
        **kwargs,
    ) -> LLMResponse:
        """Send a completion request and return the full response.

        Args:
            prompt: The prompt text to send to the model. Ignored when
                ``messages`` is provided.
            model: The model identifier (e.g. "gpt-4o", "claude-3-5-sonnet").
            messages: Full chat history as a list of ``{"role", "content"}``
                dicts. When provided, ``prompt`` is ignored and the messages
                list is sent verbatim (after provider-specific normalisation).
            **kwargs: Provider-specific extra parameters (temperature, max_tokens, etc.).

        Returns:
            LLMResponse with text, usage, model, and finish_reason.

        Raises:
            LLMError: On any provider failure.
        """
        ...

    async def stream(
        self,
        prompt: str,
        model: str,
        *,
        messages: list[dict] | None = None,
        **kwargs,
    ) -> AsyncGenerator[LLMChunk, None]:
        """Stream a completion response chunk by chunk.

        Args:
            prompt: The prompt text to send to the model. Ignored when
                ``messages`` is provided.
            model: The model identifier.
            messages: Full chat history. When provided, ``prompt`` is ignored.
            **kwargs: Provider-specific extra parameters.

        Yields:
            LLMChunk instances until done=True.

        Raises:
            LLMError: On any provider failure.
        """
        ...
        yield  # type: ignore[misc]


__all__ = ["LLMProvider"]
