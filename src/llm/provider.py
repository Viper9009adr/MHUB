"""LLM provider protocol and typed errors."""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator
from typing import Protocol

from src.llm.models import LLMChunk, LLMResponse


class LLMProvider(Protocol):
    """Protocol that all LLM provider implementations must satisfy."""

    async def complete(self, prompt: str, model: str, **kwargs) -> LLMResponse:
        """Send a completion request and return the full response.

        Args:
            prompt: The prompt text to send to the model.
            model: The model identifier (e.g. "gpt-4o", "claude-3-5-sonnet").
            **kwargs: Provider-specific extra parameters (temperature, max_tokens, etc.).

        Returns:
            LLMResponse with text, usage, model, and finish_reason.

        Raises:
            LLMError: On any provider failure.
        """
        ...

    async def stream(self, prompt: str, model: str, **kwargs) -> AsyncGenerator[LLMChunk, None]:
        """Stream a completion response chunk by chunk.

        Args:
            prompt: The prompt text to send to the model.
            model: The model identifier.
            **kwargs: Provider-specific extra parameters.

        Yields:
            LLMChunk instances until done=True.

        Raises:
            LLMError: On any provider failure.
        """
        ...
        yield  # type: ignore[misc]


__all__ = ["LLMProvider"]
