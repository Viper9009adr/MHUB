"""Anthropic LLM provider implementation via httpx."""

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

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
DEFAULT_TIMEOUT = 60.0
ANTHROPIC_VERSION = "2023-06-01"


class AnthropicProvider:
    """Anthropic Claude LLM provider using httpx."""

    def __init__(
        self,
        api_key: str,
        base_url: str = ANTHROPIC_API_URL,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    @staticmethod
    def _build_anthropic_messages(
        messages: list[dict],
    ) -> tuple[str, list[dict]]:
        """Split a standard messages list into Anthropic's (system, messages) format.

        Anthropic does not accept role=system inside the messages array.
        System turns are concatenated and returned as the top-level system string.
        """
        system_parts: list[str] = []
        chat: list[dict] = []
        for msg in messages:
            if msg.get("role") == "system":
                system_parts.append(msg.get("content", ""))
            else:
                chat.append(msg)
        return "\n\n".join(system_parts), chat

    @with_llm_retry()
    async def complete(
        self,
        prompt: str,
        model: str,
        *,
        messages: list[dict] | None = None,
        **kwargs: Any,
    ) -> LLMResponse:
        """Send a completion request to Anthropic."""
        headers = {
            "x-api-key": self._api_key,
            "anthropic-version": ANTHROPIC_VERSION,
            "content-type": "application/json",
        }
        if messages is not None:
            system_text, chat_messages = self._build_anthropic_messages(messages)
        else:
            system_text = ""
            chat_messages = [{"role": "user", "content": prompt}]

        body: dict[str, Any] = {
            "model": model,
            "messages": chat_messages,
            "max_tokens": kwargs.pop("max_tokens", 4096),
        }
        if system_text:
            body["system"] = system_text
        body.update(kwargs)

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            try:
                resp = await client.post(self._base_url, headers=headers, json=body)
            except httpx.TimeoutException as exc:
                raise LLMTimeoutError(f"Anthropic request timed out: {exc}") from exc
            except httpx.RequestError as exc:
                raise LLMProviderError(f"Anthropic request failed: {exc}") from exc

            return self._handle_response(resp)

    async def stream(
        self,
        prompt: str,
        model: str,
        *,
        messages: list[dict] | None = None,
        **kwargs: Any,
    ) -> AsyncGenerator[LLMChunk, None]:
        """Stream a completion response from Anthropic."""
        headers = {
            "x-api-key": self._api_key,
            "anthropic-version": ANTHROPIC_VERSION,
            "content-type": "application/json",
            "accept": "text/event-stream",
        }
        if messages is not None:
            system_text, chat_messages = self._build_anthropic_messages(messages)
        else:
            system_text = ""
            chat_messages = [{"role": "user", "content": prompt}]

        body: dict[str, Any] = {
            "model": model,
            "messages": chat_messages,
            "max_tokens": kwargs.pop("max_tokens", 4096),
            "stream": True,
        }
        if system_text:
            body["system"] = system_text
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
                        raise LLMTimeoutError(f"Anthropic stream timed out: {exc}") from exc
                    except httpx.HTTPStatusError as exc:
                        self._handle_http_error(exc)
                    except httpx.RequestError as exc:
                        raise LLMProviderError(
                            f"Anthropic stream request failed: {exc}"
                        ) from exc
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
            raise LLMAuthenticationError("Anthropic authentication failed")
        if resp.status_code == 429:
            raise LLMRateLimitError("Anthropic rate limit exceeded")
        if resp.status_code >= 400:
            raise LLMProviderError(
                f"Anthropic error {resp.status_code}: {resp.text}"
            )

        data = resp.json()
        content_blocks = data.get("content", [])
        text = ""
        for block in content_blocks:
            if block.get("type") == "text":
                text += block.get("text", "")

        usage_data = data.get("usage", {})
        return LLMResponse(
            text=text,
            usage=Usage(
                prompt_tokens=usage_data.get("input_tokens", 0),
                completion_tokens=usage_data.get("output_tokens", 0),
            ),
            model=data.get("model", ""),
            finish_reason=data.get("stop_reason", ""),
        )

    @staticmethod
    def _handle_http_error(exc: httpx.HTTPStatusError) -> None:
        """Convert HTTP status errors to typed LLM errors."""
        if exc.response.status_code == 401:
            raise LLMAuthenticationError(
                "Anthropic authentication failed"
            ) from exc
        if exc.response.status_code == 429:
            raise LLMRateLimitError(
                "Anthropic rate limit exceeded"
            ) from exc
        raise LLMProviderError(
            f"Anthropic HTTP {exc.response.status_code}: {exc.response.text}"
        ) from exc


__all__ = ["AnthropicProvider"]
