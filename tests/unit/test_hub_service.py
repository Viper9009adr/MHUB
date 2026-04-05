"""Unit tests for the HubService gRPC servicer."""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest

from src.hub.hub_pb2 import (
    CreateHubRequest,
    TerminateHubRequest,
    JoinHubRequest,
    TapHubRequest,
    CreateCheckpointRequest,
    RollbackCheckpointRequest,
    HubStatusRequest,
)
from src.hub.service import HubService


@pytest.fixture
def service() -> HubService:
    """Create a fresh HubService instance."""
    return HubService()


@pytest.fixture
def mock_context() -> MagicMock:
    """Create a mock gRPC servicer context."""
    ctx = MagicMock(spec=grpc.ServicerContext)
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


def test_create_hub(service: HubService, mock_context: MagicMock) -> None:
    """Test creating a hub."""
    request = CreateHubRequest(workspace_id="ws-1", initiator="user-1")
    response = service.CreateHub(request, mock_context)
    assert response.hub_id
    assert response.created_at > 0
    mock_context.set_code.assert_not_called()


def test_terminate_hub(service: HubService, mock_context: MagicMock) -> None:
    """Test terminating an existing hub."""
    create_resp = service.CreateHub(
        CreateHubRequest(workspace_id="ws-1", initiator="user-1"), mock_context
    )
    response = service.TerminateHub(
        TerminateHubRequest(hub_id=create_resp.hub_id, reason="test"), mock_context
    )
    assert response.ok is True


def test_terminate_hub_not_found(service: HubService, mock_context: MagicMock) -> None:
    """Test terminating a non-existent hub."""
    response = service.TerminateHub(
        TerminateHubRequest(hub_id="nonexistent", reason="test"), mock_context
    )
    assert response.ok is False
    mock_context.set_code.assert_called_once_with(grpc.StatusCode.NOT_FOUND)


def test_join_hub_streaming(service: HubService, mock_context: MagicMock) -> None:
    """Test bidirectional streaming JoinHub."""
    create_resp = service.CreateHub(
        CreateHubRequest(workspace_id="ws-1", initiator="user-1"), mock_context
    )

    def request_iterator() -> list[JoinHubRequest]:
        return [
            JoinHubRequest(hub_id=create_resp.hub_id, member_id=f"m-{i}", event="join")
            for i in range(3)
        ]

    events = list(service.JoinHub(iter(request_iterator()), mock_context))
    assert len(events) == 3
    assert events[0].member_id == "m-0"


def test_tap_hub(service: HubService, mock_context: MagicMock) -> None:
    """Test tapping into hub events."""
    create_resp = service.CreateHub(
        CreateHubRequest(workspace_id="ws-1", initiator="user-1"), mock_context
    )

    # Generate events
    requests = [
        JoinHubRequest(hub_id=create_resp.hub_id, member_id=f"m-{i}", event="join")
        for i in range(5)
    ]
    list(service.JoinHub(iter(requests), mock_context))

    response = service.TapHub(
        TapHubRequest(hub_id=create_resp.hub_id, limit=2), mock_context
    )
    assert len(response.events) == 2


def test_tap_hub_no_limit(service: HubService, mock_context: MagicMock) -> None:
    """Test tapping with no limit returns all events."""
    create_resp = service.CreateHub(
        CreateHubRequest(workspace_id="ws-1", initiator="user-1"), mock_context
    )

    requests = [
        JoinHubRequest(hub_id=create_resp.hub_id, member_id=f"m-{i}", event="join")
        for i in range(3)
    ]
    list(service.JoinHub(iter(requests), mock_context))

    response = service.TapHub(
        TapHubRequest(hub_id=create_resp.hub_id, limit=0), mock_context
    )
    assert len(response.events) == 3


def test_create_checkpoint(service: HubService, mock_context: MagicMock) -> None:
    """Test creating a checkpoint."""
    create_resp = service.CreateHub(
        CreateHubRequest(workspace_id="ws-1", initiator="user-1"), mock_context
    )
    response = service.CreateCheckpoint(
        CreateCheckpointRequest(hub_id=create_resp.hub_id, label="v1"), mock_context
    )
    assert response.checkpoint_id


def test_create_checkpoint_hub_not_found(service: HubService, mock_context: MagicMock) -> None:
    """Test creating a checkpoint for non-existent hub."""
    response = service.CreateCheckpoint(
        CreateCheckpointRequest(hub_id="nonexistent", label="v1"), mock_context
    )
    assert response.checkpoint_id == ""
    mock_context.set_code.assert_called_once_with(grpc.StatusCode.NOT_FOUND)


def test_rollback_checkpoint(service: HubService, mock_context: MagicMock) -> None:
    """Test rolling back to a checkpoint."""
    create_resp = service.CreateHub(
        CreateHubRequest(workspace_id="ws-1", initiator="user-1"), mock_context
    )
    cp_resp = service.CreateCheckpoint(
        CreateCheckpointRequest(hub_id=create_resp.hub_id, label="v1"), mock_context
    )
    response = service.RollbackCheckpoint(
        RollbackCheckpointRequest(
            hub_id=create_resp.hub_id, checkpoint_id=cp_resp.checkpoint_id
        ),
        mock_context,
    )
    assert response.ok is True


def test_rollback_checkpoint_not_found(service: HubService, mock_context: MagicMock) -> None:
    """Test rolling back to non-existent checkpoint."""
    create_resp = service.CreateHub(
        CreateHubRequest(workspace_id="ws-1", initiator="user-1"), mock_context
    )
    response = service.RollbackCheckpoint(
        RollbackCheckpointRequest(hub_id=create_resp.hub_id, checkpoint_id="missing"),
        mock_context,
    )
    assert response.ok is False
    mock_context.set_code.assert_called_once_with(grpc.StatusCode.NOT_FOUND)


def test_hub_status(service: HubService, mock_context: MagicMock) -> None:
    """Test getting hub status."""
    create_resp = service.CreateHub(
        CreateHubRequest(workspace_id="ws-1", initiator="user-1"), mock_context
    )
    response = service.HubStatus(
        HubStatusRequest(hub_id=create_resp.hub_id), mock_context
    )
    assert response.hub_id == create_resp.hub_id
    assert response.state == "active"


def test_hub_status_not_found(service: HubService, mock_context: MagicMock) -> None:
    """Test getting status for non-existent hub."""
    response = service.HubStatus(
        HubStatusRequest(hub_id="nonexistent"), mock_context
    )
    assert response.state == "unknown"
    mock_context.set_code.assert_called_once_with(grpc.StatusCode.NOT_FOUND)
