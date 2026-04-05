"""Pydantic schemas for the /api/v1/run endpoint."""

from __future__ import annotations

from pydantic import BaseModel, Field


class RunRequest(BaseModel):
    """Request body for POST /api/v1/run."""

    prompt: str = Field(..., min_length=1, description="The prompt to execute.")
    session_id: str | None = Field(None, min_length=1, description="Optional session identifier.")
    metadata: dict[str, str] = Field(default_factory=dict, description="Optional key-value metadata.")


class RunResponse(BaseModel):
    """Response body returned after a successful run."""

    session_id: str
    turn_index: int
    backend: str
    output: str


class HealthResponse(BaseModel):
    """Response body for GET /health."""

    status: str = "ok"


class ErrorResponse(BaseModel):
    """Standard error envelope returned on validation or server errors."""

    detail: str


__all__ = ["ErrorResponse", "HealthResponse", "RunRequest", "RunResponse"]
