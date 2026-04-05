"""Validated command and receipt contracts for the AGENTIC CLI."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from .errors import ContractViolationError


def _require_text(value: object, name: str) -> str:
    """Return a string value or raise when the contract receives another type."""

    if not isinstance(value, str):
        raise ContractViolationError(f"{name} must be a string")
    return value


def _normalize_optional_text(value: object, name: str) -> str | None:
    """Normalize optional text fields by stripping whitespace and rejecting blanks."""

    if value is None:
        return None
    text = _require_text(value, name).strip()
    if not text:
        raise ContractViolationError(f"{name} must not be blank")
    return text


def _normalize_metadata(metadata: object) -> dict[str, str]:
    """Copy metadata into a plain string-to-string dictionary."""

    if metadata is None:
        return {}
    if not isinstance(metadata, Mapping):
        raise ContractViolationError("metadata must be a mapping")
    normalized: dict[str, str] = {}
    for key, value in metadata.items():
        normalized_key = _require_text(key, "metadata key")
        normalized_value = _require_text(value, "metadata value")
        normalized[normalized_key] = normalized_value
    return normalized


@dataclass(frozen=True)
class RunCommand:
    """Represents a validated `run` request accepted by the service layer."""

    prompt: str
    session_id: str | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Normalize user input into the stable run-command contract."""

        prompt = _require_text(self.prompt, "prompt").strip()
        if not prompt:
            raise ContractViolationError("prompt must not be blank")
        object.__setattr__(self, "prompt", prompt)
        object.__setattr__(self, "session_id", _normalize_optional_text(self.session_id, "session_id"))
        object.__setattr__(self, "metadata", _normalize_metadata(self.metadata))


@dataclass(frozen=True)
class RunReceipt:
    """Represents the stable receipt returned after a successful run."""

    session_id: str
    turn_index: int
    backend: str
    output: str


def format_run_receipt(receipt: RunReceipt) -> str:
    """Format a run receipt as the CLI's single-line key-value output."""

    return (
        f"session_id={receipt.session_id} "
        f"turn_index={receipt.turn_index} "
        f"backend={receipt.backend} "
        f"output={receipt.output}"
    )


__all__ = ["RunCommand", "RunReceipt", "format_run_receipt"]
