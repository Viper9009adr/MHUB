"""Unit tests for the fork FastAPI router (create_fork_router).

IMPORTANT: create_fork_router registers routes on a module-level APIRouter
singleton in src.api.routes.fork. Calling it multiple times would accumulate
duplicate route registrations. We therefore call it exactly once, share the
FastAPI app and TestClient for all tests, and swap the mock handler's method
return values between tests.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.routes.fork import create_fork_router
from src.storage.models import ForkScenarioRecord


def _make_record(fork_id: str = "fork-test-1") -> ForkScenarioRecord:
    """Build a minimal ForkScenarioRecord for use in mock return values."""
    return ForkScenarioRecord(
        fork_id=fork_id,
        parent_hub_id="hub-parent",
        divergence_type="state_mismatch",
        divergence_reason="counter drifted",
        hal_agent_id="hal-007",
        detected_at=1000,
        status="pending",
    )


# ---------------------------------------------------------------------------
# Shared app: create_fork_router is called exactly once for this test module.
# Each test replaces the AsyncMock's return_value / side_effect before calling
# the client so it sees the right response without re-registering routes.
# ---------------------------------------------------------------------------

_shared_handler = MagicMock()
_shared_handler.get_fork_scenario = AsyncMock(return_value=None)
_shared_handler.process_decision = AsyncMock(return_value=None)
_shared_handler.get_pending_forks = AsyncMock(return_value=[])

_shared_app = FastAPI()
_shared_app.include_router(create_fork_router(_shared_handler))
_client = TestClient(_shared_app)


# ---------------------------------------------------------------------------
# GET /api/v1/forks/{fork_id}
# ---------------------------------------------------------------------------


def test_get_fork_returns_200_when_found() -> None:
    """GET /{fork_id} returns 200 and the record dict when the fork exists."""
    record = _make_record("fork-abc")
    _shared_handler.get_fork_scenario = AsyncMock(return_value=record)

    response = _client.get("/api/v1/forks/fork-abc")

    assert response.status_code == 200
    data = response.json()
    assert data["fork_id"] == "fork-abc"
    assert data["status"] == "pending"
    assert data["parent_hub_id"] == "hub-parent"


def test_get_fork_returns_404_when_not_found() -> None:
    """GET /{fork_id} returns 404 when the fork does not exist."""
    _shared_handler.get_fork_scenario = AsyncMock(return_value=None)

    response = _client.get("/api/v1/forks/nonexistent")

    assert response.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/v1/forks/{fork_id}/decide
# ---------------------------------------------------------------------------


def test_post_decide_approve_returns_200_with_new_hub_id() -> None:
    """POST /{fork_id}/decide with 'approve' returns 200 and a new_hub_id."""
    _shared_handler.process_decision = AsyncMock(return_value="new-hub-xyz")

    response = _client.post(
        "/api/v1/forks/fork-abc/decide",
        json={"decision": "approve"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["fork_id"] == "fork-abc"
    assert data["decision"] == "approve"
    assert data["new_hub_id"] == "new-hub-xyz"


def test_post_decide_reject_returns_200_with_null_new_hub_id() -> None:
    """POST /{fork_id}/decide with 'reject' returns 200 and new_hub_id=null."""
    _shared_handler.process_decision = AsyncMock(return_value=None)

    response = _client.post(
        "/api/v1/forks/fork-abc/decide",
        json={"decision": "reject"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["decision"] == "reject"
    assert data["new_hub_id"] is None


def test_post_decide_invalid_decision_returns_400() -> None:
    """POST /{fork_id}/decide with an invalid decision returns 400."""
    # decision validation happens before process_decision is called
    _shared_handler.process_decision = AsyncMock(return_value=None)

    response = _client.post(
        "/api/v1/forks/fork-abc/decide",
        json={"decision": "maybe"},
    )

    assert response.status_code == 400


def test_list_pending_forks_returns_200_empty_list() -> None:
    """GET /api/v1/forks returns 200 and [] when no pending forks exist."""
    _shared_handler.get_pending_forks = AsyncMock(return_value=[])

    response = _client.get("/api/v1/forks")

    assert response.status_code == 200
    assert response.json() == []


def test_list_pending_forks_returns_pending_records() -> None:
    """GET /api/v1/forks returns serialised pending ForkScenarioRecords."""
    records = [_make_record("fork-p1"), _make_record("fork-p2")]
    _shared_handler.get_pending_forks = AsyncMock(return_value=records)

    response = _client.get("/api/v1/forks")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["fork_id"] == "fork-p1"
    assert data[1]["fork_id"] == "fork-p2"
    assert all(item["status"] == "pending" for item in data)
