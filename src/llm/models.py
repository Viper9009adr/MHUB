"""LLM response and chunk data models."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Usage:
    """Token usage information from an LLM response."""

    prompt_tokens: int
    completion_tokens: int

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens


@dataclass(frozen=True)
class LLMResponse:
    """Complete response from an LLM provider."""

    text: str
    usage: Usage
    model: str
    finish_reason: str


@dataclass(frozen=True)
class LLMChunk:
    """A single streaming chunk from an LLM provider."""

    text: str
    index: int
    done: bool


__all__ = ["LLMChunk", "LLMResponse", "Usage"]
