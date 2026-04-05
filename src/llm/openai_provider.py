"""OpenAI LLM provider implementation via httpx."""

from __future__ import annotations

import asyncio
import json
import logging
import random
from collections.abc import AsyncGenerator, AsyncIterator
from typing import Any

import httpx

from src.llm.errors import (
    LLMAuthenticationError,
    LLMError,
    LLMProviderError,
    LLMRateLimitError,
    LLMTimeoutError,
)
from src.llm.models import LLMChunk, LLMResponse, Usage
from src.llm.retry import with_llm_retry
from src.llm.sse_parser import parse_sse_chunk

logger = logging.getLogger(__name__)

OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
DEFAULT_TIMEOUT = 60.0


class OpenAIProvider:
    """OpenAI-compatible LLM provider using httpx.

    Works with any OpenAI-compatible API endpoint (OpenAI, Azure,
    local models via OpenAI-compatible servers).
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = OPENAI_API_URL,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    @with_llm_retry()
    async def complete(
        self, prompt: str, model: str, **kwargs: Any
    ) -> LLMResponse:
        """Send a completion request to OpenAI."""
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        body: dict[str, Any] = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
        }
        body.update(kwargs)

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            try:
                resp = await client.post(self._base_url, headers=headers, json=body)
            except httpx.TimeoutException as exc:
                raise LLMTimeoutError(f"OpenAI request timed out: {exc}") from exc
            except httpx.RequestError as exc:
                raise LLMProviderError(f"OpenAI request failed: {exc}") from exc

            return self._handle_response(resp)

    async def stream(
        self, prompt: str, model: str, **kwargs: Any
    ) -> AsyncGenerator[LLMChunk, None]:
        """Stream a completion response from OpenAI."""
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        }
        body: dict[str, Any] = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": True,
        }
        body.update(kwargs)

        _last_exc: Exception | None = None
        for _attempt in range(3):
            _yielded = False
            try:
                async with httpx.AsyncClient(timeout=self._timeout) as client:
                    try:
                        async with client.stream("POST", self._base_url, headers=headers, json=body) as resp:
                            resp.raise_for_status()
                            index = 0
                            async for line in resp.aiter_lines():
                                line = line.strip()
                                if not line or not line.startswith("data: "):
                                    continue
                                data_str = line[6:]
                                if data_str == "[DONE]":
                                    yield LLMChunk(text="", index=index, done=True)
                                    return
                                text = parse_sse_chunk(data_str)
                                if text:
                                    index += 1
                                    _yielded = True
                                    yield LLMChunk(text=text, index=index, done=False)
                    except httpx.TimeoutException as exc:
                        raise LLMTimeoutError(f"OpenAI stream timed out: {exc}") from exc
                    except httpx.HTTPStatusError as exc:
                        self._handle_http_error(exc)
                    except httpx.RequestError as exc:
                        raise LLMProviderError(f"OpenAI stream request failed: {exc}") from exc
            except (LLMRateLimitError, LLMTimeoutError) as exc:
                _last_exc = exc
                if _yielded:
                    raise
                if _attempt < 2:
                    _delay = min(1.0 * (2.0 ** _attempt), 30.0)
                    _delay = random.uniform(0, _delay)
                    await asyncio.sleep(_delay)
                continue
            return
        if _last_exc is not None:
            raise _last_exc

    def _handle_response(self, resp: httpx.Response) -> LLMResponse:
        """Handle a non-streaming HTTP response."""
        if resp.status_code == 401:
            raise LLMAuthenticationError("OpenAI authentication failed")
        if resp.status_code == 429:
            raise LLMRateLimitError("OpenAI rate limit exceeded")
        if resp.status_code >= 400:
            raise LLMProviderError(
                f"OpenAI error {resp.status_code}: {resp.text}"
            )

        data = resp.json()
        choice = data.get("choices", [{}])[0]
        message = choice.get("message", {})
        usage_data = data.get("usage", {})

        return LLMResponse(
            text=message.get("content", ""),
            usage=Usage(
                prompt_tokens=usage_data.get("prompt_tokens", 0),
                completion_tokens=usage_data.get("completion_tokens", 0),
            ),
            model=data.get("model", ""),
            finish_reason=choice.get("finish_reason", ""),
        )

    @staticmethod
    def _handle_http_error(exc: httpx.HTTPStatusError) -> None:
        """Convert HTTP status errors to typed LLM errors."""
        if exc.response.status_code == 401:
            raise LLMAuthenticationError("OpenAI authentication failed") from exc
        if exc.response.status_code == 429:
            raise LLMRateLimitError("OpenAI rate limit exceeded") from exc
        raise LLMProviderError(
            f"OpenAI HTTP {exc.response.status_code}: {exc.response.text}"
        ) from exc


__all__ = ["OpenAIProvider"]
