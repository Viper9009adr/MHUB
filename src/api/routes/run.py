"""FastAPI route handlers for the /api/v1/run endpoint."""

from __future__ import annotations

from fastapi import APIRouter

from src.api.schemas.run import RunRequest, RunResponse
from src.orc.dispatch import OrcDispatch

router = APIRouter(prefix="/api/v1", tags=["run"])


def create_run_router(dispatch: OrcDispatch) -> APIRouter:
    """Build the run router with an injected OrcDispatch dependency.

    Args:
        dispatch: The orc dispatch instance to use for run requests.

    Returns:
        Configured APIRouter with run endpoints.
    """

    @router.post("/run", response_model=RunResponse)
    async def post_run(request: RunRequest) -> RunResponse:
        """Execute a run request and return the receipt."""
        receipt = await dispatch.dispatch_run(
            prompt=request.prompt,
            session_id=request.session_id,
            metadata=request.metadata,
        )
        return RunResponse(
            session_id=receipt.session_id,
            turn_index=receipt.turn_index,
            backend=receipt.backend,
            output=receipt.output,
        )

    return router


__all__ = ["create_run_router"]
