"""Error hierarchy for the AGENTIC CLI Python implementation."""

from __future__ import annotations


class AgenticCliError(Exception):
    """Base error for local agentic cli failures."""


class ContractViolationError(AgenticCliError):
    """Raised when a CLI/service contract is invalid."""


class BackendExecutionError(AgenticCliError):
    """Raised when the backend cannot complete a request."""


__all__ = [
    "AgenticCliError",
    "BackendExecutionError",
    "ContractViolationError",
]
