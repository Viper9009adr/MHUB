"""LLM error hierarchy."""

from __future__ import annotations


class LLMError(Exception):
    """Base error for all LLM provider failures."""


class LLMRateLimitError(LLMError):
    """Raised when the provider returns a rate-limit response (429)."""


class LLMTimeoutError(LLMError):
    """Raised when a provider request exceeds the configured timeout."""


class LLMProviderError(LLMError):
    """Raised when the provider returns an unexpected error (5xx, malformed)."""


class LLMAuthenticationError(LLMError):
    """Raised when authentication with the provider fails (401/403)."""


__all__ = [
    "LLMError",
    "LLMRateLimitError",
    "LLMTimeoutError",
    "LLMProviderError",
    "LLMAuthenticationError",
]
