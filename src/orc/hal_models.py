"""HAL agent data models."""

from __future__ import annotations

from pydantic import BaseModel, Field


class Span(BaseModel):
    """A text span flagged for potential hallucination."""

    start: int = Field(..., description="Start character offset in the response.")
    end: int = Field(..., description="End character offset in the response.")
    text: str = Field(..., description="The flagged text content.")
    issue: str = Field(..., description="Description of the issue.")


class HallucinationReport(BaseModel):
    """Report from the HallucinationScannerAgent."""

    run_id: str = Field(..., description="The run this report analyses.")
    hallucination_score: float = Field(
        ..., ge=0.0, le=1.0, description="Overall hallucination score (0.0=clean, 1.0=severe)."
    )
    spans: list[Span] = Field(default_factory=list, description="Flagged text spans.")
    summary: str = Field(..., description="Human-readable summary of findings.")


__all__ = ["HallucinationReport", "Span"]
