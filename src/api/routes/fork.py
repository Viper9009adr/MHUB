"""FastAPI router for fork scenario endpoints.

Provides REST endpoints for querying fork scenarios and submitting
user decisions.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.orc.fork_handler import ForkHandler

router = APIRouter(prefix="/api/v1/forks", tags=["forks"])


class DecideRequest(BaseModel):
    """Request body for submitting a fork decision."""

    decision: str


def create_fork_router(handler: ForkHandler) -> APIRouter:
    """Build the fork router with an injected ForkHandler dependency.

    Args:
        handler: The ForkHandler instance to use for processing.

    Returns:
        Configured APIRouter with fork endpoints.
    """

    @router.get("")
    async def list_pending_forks() -> list[dict]:
        """List all pending fork scenarios.

        Returns:
            List of fork scenario dicts with status=='pending'.
        """
        records = await handler.get_pending_forks()
        return [
            {
                "fork_id": r.fork_id,
                "parent_hub_id": r.parent_hub_id,
                "divergence_type": r.divergence_type,
                "divergence_reason": r.divergence_reason,
                "hal_agent_id": r.hal_agent_id,
                "detected_at": r.detected_at,
                "status": r.status,
                "user_decision": r.user_decision,
                "decided_at": r.decided_at,
                "new_hub_id": r.new_hub_id,
            }
            for r in records
        ]

    @router.get("/{fork_id}")
    async def get_fork(fork_id: str) -> dict:
        """Get details of a fork scenario by ID.

        Args:
            fork_id: The fork scenario identifier.

        Returns:
            Dictionary with fork scenario details.

        Raises:
            HTTPException: 404 if fork not found.
        """
        record = await handler.get_fork_scenario(fork_id)

        if record is None:
            raise HTTPException(status_code=404, detail=f"Fork {fork_id} not found")

        return {
            "fork_id": record.fork_id,
            "parent_hub_id": record.parent_hub_id,
            "divergence_type": record.divergence_type,
            "divergence_reason": record.divergence_reason,
            "hal_agent_id": record.hal_agent_id,
            "detected_at": record.detected_at,
            "status": record.status,
            "user_decision": record.user_decision,
            "decided_at": record.decided_at,
            "new_hub_id": record.new_hub_id,
        }

    @router.post("/{fork_id}/decide")
    async def post_decide(fork_id: str, body: DecideRequest) -> dict:
        """Submit a decision for a fork scenario.

        Args:
            fork_id: The fork scenario identifier.
            body: Decision request with 'approve' or 'reject'.

        Returns:
            Dictionary with decision result.

        Raises:
            HTTPException: 400 if decision is invalid.
        """
        if body.decision not in ("approve", "reject"):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid decision: {body.decision!r}. Must be 'approve' or 'reject'.",
            )

        try:
            new_hub_id = await handler.process_decision(fork_id, body.decision)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        return {
            "fork_id": fork_id,
            "decision": body.decision,
            "new_hub_id": new_hub_id,
        }

    return router


__all__ = ["create_fork_router"]
