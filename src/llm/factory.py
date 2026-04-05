"""LLM provider factory.

Creates the appropriate provider instance based on provider name.
"""

from __future__ import annotations

import os
from typing import Any

from src.llm.provider import LLMProvider

_PROVIDER_REGISTRY: dict[str, type] = {}


def _register(name: str) -> Any:
    """Decorator to register a provider class."""

    def decorator(cls: type) -> type:
        _PROVIDER_REGISTRY[name] = cls
        return cls

    return decorator


# Import and register providers (lazy to avoid circular imports)
def _ensure_registry() -> None:
    """Lazy-load provider classes into the registry."""
    if _PROVIDER_REGISTRY:
        return
    from src.llm.openai_provider import OpenAIProvider
    from src.llm.anthropic_provider import AnthropicProvider
    from src.llm.nvidia_provider import NvidiaProvider
    from src.llm.openrouter_provider import OpenRouterProvider

    _register("openai")(OpenAIProvider)
    _register("anthropic")(AnthropicProvider)
    _register("nvidia")(NvidiaProvider)
    _register("openrouter")(OpenRouterProvider)


def create_provider(
    provider_name: str,
    api_key: str | None = None,
    model: str | None = None,
    base_url: str | None = None,
    **kwargs: Any,
) -> LLMProvider:
    """Create an LLM provider instance by name.

    Args:
        provider_name: Provider identifier (openai, anthropic, nvidia, openrouter).
        api_key: API key for the provider. If None, reads from environment.
        model: Default model identifier (stored for reference, not used by factory).
        base_url: Optional override for the provider API endpoint.
        **kwargs: Additional provider-specific configuration.

    Returns:
        An LLMProvider instance.

    Raises:
        ValueError: If the provider name is unknown.
        EnvironmentError: If no API key is available.
    """
    _ensure_registry()

    name = provider_name.lower()
    if name not in _PROVIDER_REGISTRY:
        raise ValueError(
            f"Unknown LLM provider: {name!r}. "
            f"Available: {sorted(_PROVIDER_REGISTRY.keys())}"
        )

    # Resolve API key from arg, env var, or raise
    env_key_map = {
        "openai": "OPENAI_API_KEY",
        "anthropic": "ANTHROPIC_API_KEY",
        "nvidia": "NVIDIA_API_KEY",
        "openrouter": "OPENROUTER_API_KEY",
    }
    env_var = env_key_map.get(name, f"{name.upper()}_API_KEY")
    resolved_key = api_key or os.environ.get(env_var)
    if not resolved_key:
        raise EnvironmentError(
            f"No API key for provider {name!r}. "
            f"Set {env_var} or pass api_key=..."
        )

    provider_cls = _PROVIDER_REGISTRY[name]

    # Build constructor args
    ctor_kwargs: dict[str, Any] = {"api_key": resolved_key}
    if base_url is not None:
        ctor_kwargs["base_url"] = base_url
    ctor_kwargs.update(kwargs)

    return provider_cls(**ctor_kwargs)


def list_providers() -> list[str]:
    """Return list of registered provider names."""
    _ensure_registry()
    return sorted(_PROVIDER_REGISTRY.keys())


__all__ = ["create_provider", "list_providers"]
