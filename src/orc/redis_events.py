"""Typed Redis event schemas using Pydantic.

Defines the structured event types exchanged over Redis pub/sub
between the orchestrator, HAL agent, and API layer.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class OrchEvent(BaseModel):
    """Generic orchestrator event published to Redis."""

    run_id: str = Field(..., description="Unique run identifier.")
    type: str = Field(..., description="Event type (started, completed, error, cancelled).")
    status: str = Field(..., description="Current run status.")
    payload: dict[str, str] = Field(default_factory=dict, description="Event-specific data.")
    ts: float = Field(..., description="Unix timestamp of the event.")


class RunStartedEvent(OrchEvent):
    """Published when a run begins execution."""

    type: str = "started"
    status: str = "running"


class RunCompletedEvent(OrchEvent):
    """Published when a run finishes successfully."""

    type: str = "completed"
    status: str = "completed"
    output: str = Field(default="", description="Run output text.")


class RunErrorEvent(OrchEvent):
    """Published when a run fails with an error."""

    type: str = "error"
    status: str = "error"
    error: str = Field(default="", description="Error message.")
    error_detail: str = Field(default="", description="Detailed error information.")


class RunCancelledEvent(OrchEvent):
    """Published when a run is cancelled."""

    type: str = "cancelled"
    status: str = "cancelled"


class ForkEvent(BaseModel):
    """Fork divergence event published by HAL agent."""

    fork_id: str = Field(..., description="Unique fork identifier.")
    parent_hub_id: str = Field(..., description="Parent hub that diverged.")
    divergence_type: str = Field(..., description="Type of divergence.")
    divergence_reason: str = Field(..., description="Human-readable reason.")
    hal_agent_id: str = Field(..., description="HAL agent that detected the fork.")
    detected_at: int = Field(..., description="Unix timestamp of detection.")


class HalReportEvent(BaseModel):
    """Hallucination report event published by HAL agent."""

    run_id: str = Field(..., description="The analysed run.")
    hallucination_score: float = Field(..., ge=0.0, le=1.0)
    summary: str = Field(..., description="Analysis summary.")
    span_count: int = Field(default=0, description="Number of flagged spans.")


__all__ = [
    "ForkEvent",
    "HalReportEvent",
    "OrchEvent",
    "RunCancelledEvent",
    "RunCompletedEvent",
    "RunErrorEvent",
    "RunStartedEvent",
]
