# LLM Provider Setup Guide

## Overview

Meridian HUB supports four LLM providers through a unified `LLMProvider` protocol:

- **OpenAI** — GPT-4o, GPT-4o Mini, and compatible endpoints
- **Anthropic** — Claude 3.5 Sonnet, Claude 3.5 Haiku
- **Nvidia NIM** — Llama 3.1 405B and other NIM-hosted models
- **OpenRouter** — Unified gateway to multiple providers

## Quick Start

### 1. Set your API key

```bash
# OpenAI
export OPENAI_API_KEY="sk-..."

# Anthropic
export ANTHROPIC_API_KEY="sk-ant-..."

# Nvidia
export NVIDIA_API_KEY="nvapi-..."

# OpenRouter
export OPENROUTER_API_KEY="sk-or-..."
```

### 2. Configure the provider

```bash
# In .env
LLM_PROVIDER=openai
LLM_MODEL=gpt-4o
```

### 3. Use in code

```python
from src.llm.factory import create_provider

provider = create_provider("openai")
response = await provider.complete(
    prompt="Hello, world!",
    model="gpt-4o"
)
print(response.text)
```

## Provider Details

### OpenAI

```python
from src.llm.openai_provider import OpenAIProvider

provider = OpenAIProvider(
    api_key="sk-...",
    base_url="https://api.openai.com/v1/chat/completions",  # default
    timeout=60.0,
)
```

Compatible with any OpenAI-compatible API (Azure, local models via Ollama/LM Studio, etc.).

### Anthropic

```python
from src.llm.anthropic_provider import AnthropicProvider

provider = AnthropicProvider(
    api_key="sk-ant-...",
    base_url="https://api.anthropic.com/v1/messages",  # default
    timeout=60.0,
)
```

### Nvidia NIM

```python
from src.llm.nvidia_provider import NvidiaProvider

provider = NvidiaProvider(
    api_key="nvapi-...",
    base_url="https://integrate.api.nvidia.com/v1/chat/completions",  # default
    timeout=60.0,
)
```

Also works with self-hosted NIM instances — just override `base_url`.

### OpenRouter

```python
from src.llm.openrouter_provider import OpenRouterProvider

provider = OpenRouterProvider(
    api_key="sk-or-...",
    base_url="https://openrouter.ai/api/v1/chat/completions",  # default
    timeout=60.0,
)
```

OpenRouter model IDs use the format `provider/model`, e.g. `anthropic/claude-3.5-sonnet`.

## Streaming

All providers support streaming responses:

```python
async for chunk in provider.stream(prompt="...", model="gpt-4o"):
    if chunk.done:
        break
    print(chunk.text, end="", flush=True)
```

## Error Handling

The provider layer defines a typed error hierarchy:

| Error | When |
|---|---|
| `LLMError` | Base class for all LLM errors |
| `LLMRateLimitError` | HTTP 429 from provider |
| `LLMTimeoutError` | Request exceeds configured timeout |
| `LLMProviderError` | HTTP 5xx or malformed response |
| `LLMAuthenticationError` | HTTP 401/403 |

## Retry

LLM calls are automatically retried on rate-limit and timeout errors:

- **Max attempts**: 3
- **Back-off**: Exponential (1s, 2s, 4s) with jitter
- **Max delay**: 30s

Configure via `@with_llm_retry(config=LLMRetryConfig(...))`.

## Factory

Use `create_provider()` for dynamic provider selection:

```python
from src.llm.factory import create_provider, list_providers

print(list_providers())  # ['anthropic', 'nvidia', 'openai', 'openrouter']

provider = create_provider(
    provider_name="anthropic",
    api_key="sk-ant-...",  # or set ANTHROPIC_API_KEY env var
)
```
