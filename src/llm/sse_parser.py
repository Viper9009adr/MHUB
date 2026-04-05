"""SSE (Server-Sent Events) line parser for streaming LLM responses.

Parses raw SSE text into structured chunks compatible with LLMChunk.
Handles the standard SSE format:
    data: {"choices": [{"delta": {"content": "text"}}]}
    data: [DONE]
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SSEEvent:
    """A parsed SSE event."""

    event_type: str | None  # "event:" field, if present
    data: str  # "data:" field content
    id: str | None  # "id:" field, if present
    retry: int | None  # "retry:" field, if present


def parse_sse_line(line: str) -> str | None:
    """Parse a single SSE data line and return the payload string.

    Args:
        line: A raw SSE line (e.g. "data: {\"text\": \"hello\"}").

    Returns:
        The data payload string, or None if the line is not a data line.
    """
    if line.startswith("data: "):
        return line[6:]
    if line.startswith("data:"):
        return line[5:]
    return None


def parse_sse_chunk(data: str) -> str | None:
    """Parse an SSE data payload into text content.

    Handles common provider formats:
    - OpenAI: {"choices": [{"delta": {"content": "text"}}]}
    - Anthropic: {"delta": {"text": "text"}}
    - Generic: {"text": "text"} or bare string
    - [DONE] sentinel

    Args:
        data: The raw data string from an SSE event.

    Returns:
        Extracted text content, or None if this is a [DONE] sentinel
        or the payload cannot be parsed.
    """
    data = data.strip()
    if data == "[DONE]":
        return None

    try:
        payload = json.loads(data)
    except (json.JSONDecodeError, ValueError):
        # Bare string or unparseable — return as-is if non-empty
        return data if data else None

    # OpenAI format
    choices = payload.get("choices")
    if isinstance(choices, list) and len(choices) > 0:
        choice = choices[0]
        delta = choice.get("delta", {})
        content = delta.get("content")
        if content is not None:
            return str(content)
        # Check finish_reason
        finish = choice.get("finish_reason")
        if finish is not None:
            return None

    # Anthropic format
    delta = payload.get("delta")
    if isinstance(delta, dict):
        text = delta.get("text")
        if text is not None:
            return str(text)

    # Generic format
    text = payload.get("text")
    if text is not None:
        return str(text)

    # Content field (some providers)
    content = payload.get("content")
    if content is not None:
        return str(content)

    return None


def parse_sse_stream(raw: str) -> list[str]:
    """Parse a complete SSE stream into a list of text chunks.

    Args:
        raw: The full raw SSE response body.

    Returns:
        List of text content strings extracted from data lines.
    """
    chunks: list[str] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        data = parse_sse_line(line)
        if data is not None:
            text = parse_sse_chunk(data)
            if text is not None:
                chunks.append(text)
    return chunks


__all__ = ["SSEEvent", "parse_sse_chunk", "parse_sse_line", "parse_sse_stream"]
