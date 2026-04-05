"""Data models for the Meridian HUB storage layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class HubRecord:
    """Represents a hub entity persisted to storage."""

    hub_id: str
    workspace_id: str
    initiator: str
    state: str = "active"
    created_at: int = 0
    terminated_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to a dictionary suitable for database insertion."""
        return {
            "hub_id": self.hub_id,
            "workspace_id": self.workspace_id,
            "initiator": self.initiator,
            "state": self.state,
            "created_at": self.created_at,
            "terminated_reason": self.terminated_reason,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HubRecord":
        """Construct a HubRecord from a dictionary."""
        return cls(
            hub_id=data["hub_id"],
            workspace_id=data["workspace_id"],
            initiator=data["initiator"],
            state=data.get("state", "active"),
            created_at=data.get("created_at", 0),
            terminated_reason=data.get("terminated_reason"),
        )


@dataclass
class CheckpointRecord:
    """Represents a checkpoint entity persisted to storage."""

    checkpoint_id: str
    hub_id: str
    label: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to a dictionary suitable for database insertion."""
        return {
            "checkpoint_id": self.checkpoint_id,
            "hub_id": self.hub_id,
            "label": self.label,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CheckpointRecord":
        """Construct a CheckpointRecord from a dictionary."""
        return cls(
            checkpoint_id=data["checkpoint_id"],
            hub_id=data["hub_id"],
            label=data["label"],
        )


@dataclass
class ForkScenarioRecord:
    """Represents a fork divergence scenario persisted to storage."""

    fork_id: str
    parent_hub_id: str
    divergence_type: str
    divergence_reason: str
    hal_agent_id: str
    detected_at: int = 0
    status: str = "pending"
    user_decision: str | None = None
    decided_at: int | None = None
    new_hub_id: str | None = None
    divergence_payload: bytes | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to a dictionary suitable for database insertion."""
        return {
            "fork_id": self.fork_id,
            "parent_hub_id": self.parent_hub_id,
            "divergence_type": self.divergence_type,
            "divergence_reason": self.divergence_reason,
            "hal_agent_id": self.hal_agent_id,
            "detected_at": self.detected_at,
            "status": self.status,
            "user_decision": self.user_decision,
            "decided_at": self.decided_at,
            "new_hub_id": self.new_hub_id,
            "divergence_payload": self.divergence_payload,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ForkScenarioRecord":
        """Construct a ForkScenarioRecord from a dictionary."""
        return cls(
            fork_id=data["fork_id"],
            parent_hub_id=data["parent_hub_id"],
            divergence_type=data["divergence_type"],
            divergence_reason=data["divergence_reason"],
            hal_agent_id=data["hal_agent_id"],
            detected_at=data.get("detected_at", 0),
            status=data.get("status", "pending"),
            user_decision=data.get("user_decision"),
            decided_at=data.get("decided_at"),
            new_hub_id=data.get("new_hub_id"),
            divergence_payload=data.get("divergence_payload"),
        )


@dataclass
class AgentSessionRecord:
    """Represents an agent session persisted to storage."""

    session_id: str
    hub_id: str
    agent_id: str
    run_id: str
    status: str = "active"
    started_at: int = 0
    ended_at: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to a dictionary suitable for database insertion."""
        return {
            "session_id": self.session_id,
            "hub_id": self.hub_id,
            "agent_id": self.agent_id,
            "run_id": self.run_id,
            "status": self.status,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AgentSessionRecord":
        """Construct an AgentSessionRecord from a dictionary."""
        return cls(
            session_id=data["session_id"],
            hub_id=data["hub_id"],
            agent_id=data["agent_id"],
            run_id=data["run_id"],
            status=data.get("status", "active"),
            started_at=data.get("started_at", 0),
            ended_at=data.get("ended_at"),
        )


@dataclass
class ApprovalRequestRecord:
    """Represents an approval request persisted to storage."""

    request_id: str
    fork_id: str
    status: str = "pending"
    created_at: int = 0
    decided_at: int | None = None
    decision: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to a dictionary suitable for database insertion."""
        return {
            "request_id": self.request_id,
            "fork_id": self.fork_id,
            "status": self.status,
            "created_at": self.created_at,
            "decided_at": self.decided_at,
            "decision": self.decision,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ApprovalRequestRecord":
        """Construct an ApprovalRequestRecord from a dictionary."""
        return cls(
            request_id=data["request_id"],
            fork_id=data["fork_id"],
            status=data.get("status", "pending"),
            created_at=data.get("created_at", 0),
            decided_at=data.get("decided_at"),
            decision=data.get("decision"),
        )


@dataclass
class AuditLogRecord:
    """Represents an audit log entry persisted to storage."""

    log_id: str
    agent_id: str
    action: str
    resource: str
    timestamp: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to a dictionary suitable for database insertion."""
        return {
            "log_id": self.log_id,
            "agent_id": self.agent_id,
            "action": self.action,
            "resource": self.resource,
            "timestamp": self.timestamp,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AuditLogRecord":
        """Construct an AuditLogRecord from a dictionary."""
        return cls(
            log_id=data["log_id"],
            agent_id=data["agent_id"],
            action=data["action"],
            resource=data["resource"],
            timestamp=data.get("timestamp", 0),
            metadata=data.get("metadata", {}),
        )


__all__ = [
    "HubRecord",
    "CheckpointRecord",
    "ForkScenarioRecord",
    "AgentSessionRecord",
    "ApprovalRequestRecord",
    "AuditLogRecord",
]
