"""Fork handling for processing divergence detection events and decisions.

Orchestrates the lifecycle of a fork scenario: receiving a detection
event, persisting it, processing user decisions, and spawning new hubs
when approved.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import msgpack

from src.storage.models import ForkScenarioRecord, HubRecord
from src.orc.fork_context import set_context, get_context, clear_context
from src.orc.redis_bridge import RedisBridge, with_redis_retry

logger = logging.getLogger(__name__)

# Channel names
CHANNEL_DETECT = "meridian:fork:detect"
CHANNEL_PENDING = "meridian:fork:pending"
CHANNEL_RESOLVED = "meridian:fork:resolved"

# BYTEA format markers
FORMAT_MSGPACK = 0x01
FORMAT_JSON = 0x02

# Maximum payload size (1 MB)
MAX_PAYLOAD_SIZE = 1 * 1024 * 1024


@dataclass
class DivergenceEvent:
    """Typed container for a divergence detection event.

    Used for type safety across both Redis and gRPC paths.
    """

    parent_hub_id: str
    hal_agent_id: str
    divergence_type: str
    divergence_reason: str
    evidence: bytes = b""
    detected_at: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to a dictionary."""
        return {
            "parent_hub_id": self.parent_hub_id,
            "hal_agent_id": self.hal_agent_id,
            "divergence_type": self.divergence_type,
            "divergence_reason": self.divergence_reason,
            "evidence": self.evidence,
            "detected_at": self.detected_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DivergenceEvent":
        """Construct from a dictionary."""
        return cls(
            parent_hub_id=data["parent_hub_id"],
            hal_agent_id=data["hal_agent_id"],
            divergence_type=data["divergence_type"],
            divergence_reason=data["divergence_reason"],
            evidence=data.get("evidence", b""),
            detected_at=data.get("detected_at", 0),
        )

    @classmethod
    def from_msgpack(cls, raw: bytes) -> "DivergenceEvent":
        """Deserialize from msgpack bytes."""
        data = msgpack.unpackb(raw, raw=False)
        return cls.from_dict(data)

    def to_msgpack(self) -> bytes:
        """Serialize to msgpack bytes."""
        return msgpack.packb(self.to_dict(), use_bin_type=True)


class ForkHandler:
    """Handles fork detection events and user decisions.

    Args:
        storage: Storage backend with fork_scenarios CRUD methods.
        redis: RedisBridge for publishing notifications.
    """

    def __init__(self, storage: Any, redis: RedisBridge | None = None) -> None:
        self._storage = storage
        self._redis = redis

    async def handle_detection(self, event: DivergenceEvent) -> ForkScenarioRecord:
        """Process a divergence detection event.

        Validates the event, creates a ForkScenarioRecord, persists it,
        sets shared context, and publishes to the pending channel.

        Args:
            event: The divergence event to process.

        Returns:
            The persisted ForkScenarioRecord.
        """
        fork_id = str(uuid.uuid4())
        detected_at = event.detected_at or int(time.time())

        # Serialize evidence payload with format marker
        payload = self._serialize_payload(event.evidence)

        record = ForkScenarioRecord(
            fork_id=fork_id,
            parent_hub_id=event.parent_hub_id,
            divergence_type=event.divergence_type,
            divergence_reason=event.divergence_reason,
            hal_agent_id=event.hal_agent_id,
            detected_at=detected_at,
            status="pending",
            divergence_payload=payload,
        )

        # Persist to storage — await properly, no fire-and-forget
        try:
            await self._storage.insert_fork_scenario(record)
        except Exception as exc:
            logger.error(
                "Failed to persist fork scenario %s: %s",
                fork_id,
                exc,
            )
            raise

        # Set shared context
        set_context(fork_id, {
            "parent_hub_id": event.parent_hub_id,
            "hal_agent_id": event.hal_agent_id,
            "divergence_type": event.divergence_type,
        })

        # Publish to pending channel for user notification
        if self._redis is not None:
            await self.publish_pending(fork_id, record)

        logger.info(
            "Fork detection handled: fork_id=%s, parent_hub_id=%s",
            fork_id,
            event.parent_hub_id,
        )
        return record

    async def process_decision(self, fork_id: str, decision: str) -> str | None:
        """Process a user decision on a fork scenario.

        Args:
            fork_id: The fork scenario identifier.
            decision: Either 'approve' or 'reject'.

        Returns:
            new_hub_id if approved and spawned, None otherwise.
        """
        if decision not in ("approve", "reject"):
            raise ValueError(f"Invalid decision: {decision!r}")

        decided_at = int(time.time())

        if decision == "approve":
            new_hub_id = await self.spawn_new_hub(fork_id)
            if new_hub_id is None:
                # Spawn failed — update status to 'failed'
                await self._update_decision(fork_id, decision, None, "failed", decided_at)
                await self._publish_resolved(fork_id, "failed", error="spawn_new_hub returned None")
                return None
            await self._update_decision(fork_id, decision, new_hub_id, "approved", decided_at)
            await self._publish_resolved(fork_id, "approved", new_hub_id=new_hub_id)
            clear_context(fork_id)
            return new_hub_id
        else:
            await self._update_decision(fork_id, decision, None, "rejected", decided_at)
            await self._publish_resolved(fork_id, "rejected")
            clear_context(fork_id)
            return None

    async def spawn_new_hub(self, fork_id: str) -> str | None:
        """Spawn a new hub for an approved fork.

        Looks up the parent_hub_id from context, validates the parent
        hub exists via storage, then creates a new hub ID.

        Args:
            fork_id: The fork scenario identifier.

        Returns:
            The new hub_id on success, None on failure.
        """
        ctx = get_context(fork_id)
        if ctx is None:
            logger.error("No context found for fork_id=%s", fork_id)
            return None

        parent_hub_id = ctx.get("parent_hub_id", "")
        if not parent_hub_id:
            logger.error("No parent_hub_id in context for fork_id=%s", fork_id)
            return None

        try:
            parent = await self._storage.get_hub(parent_hub_id)
            if parent is None:
                logger.error("Parent hub %s not found", parent_hub_id)
                return None
            new_hub_id = str(uuid.uuid4())
            new_record = HubRecord(
                hub_id=new_hub_id,
                workspace_id=parent.workspace_id,
                initiator=parent.initiator,
                state="active",
                created_at=int(time.time()),
            )
            await self._storage.insert_hub(new_record)
            return new_hub_id
        except Exception as exc:
            logger.error("Failed to spawn new hub for fork_id=%s: %s", fork_id, exc)
            return None

    def _serialize_payload(self, evidence: bytes) -> bytes | None:
        """Serialize evidence payload with format marker.

        Prepends a 1-byte format marker: 0x01 for msgpack, 0x02 for json.
        Validates payload is under 1 MB.

        Evidence is stored as raw bytes with a format marker prefix —
        no double-encoding. The caller should pass raw bytes directly.

        Args:
            evidence: Raw evidence bytes.

        Returns:
            Serialized payload with format marker, or None if empty.
        """
        if not evidence:
            return None

        if len(evidence) > MAX_PAYLOAD_SIZE:
            logger.warning(
                "Evidence payload exceeds 1 MB limit (%d bytes), truncating",
                len(evidence),
            )
            evidence = evidence[:MAX_PAYLOAD_SIZE]

        return bytes([FORMAT_MSGPACK]) + evidence

    @staticmethod
    def deserialize_payload(raw: bytes) -> Any | None:
        """Deserialize a payload that was serialized with _serialize_payload.

        Reads the first byte as a format marker and dispatches to the
        appropriate deserializer.

        Args:
            raw: Raw payload bytes with format marker prefix.

        Returns:
            The deserialized evidence bytes, or None if empty/invalid.
        """
        if not raw:
            return None

        fmt = raw[0]
        body = raw[1:]

        if fmt == FORMAT_MSGPACK:
            try:
                return msgpack.unpackb(body, raw=False)
            except Exception as exc:
                logger.error("Failed to msgpack-deserialize payload: %s", exc)
                return None
        elif fmt == FORMAT_JSON:
            try:
                return json.loads(body)
            except Exception as exc:
                logger.error("Failed to json-deserialize payload: %s", exc)
                return None
        else:
            logger.warning("Unknown format marker 0x%02x in payload", fmt)
            return None

    async def _update_decision(
        self,
        fork_id: str,
        decision: str,
        new_hub_id: str | None,
        status: str,
        decided_at: int,
    ) -> None:
        """Update the fork scenario decision in storage.

        Args:
            fork_id: The fork scenario identifier.
            decision: User decision ('approve' or 'reject').
            new_hub_id: New hub ID if spawned.
            status: New status string.
            decided_at: Timestamp of decision.
        """
        try:
            await self._storage.update_fork_decision(
                fork_id, decision, new_hub_id, status, decided_at
            )
        except Exception as exc:
            logger.error(
                "Failed to update fork decision for %s: %s",
                fork_id,
                exc,
            )
            raise

    @with_redis_retry()
    async def publish_pending(self, fork_id: str, record: ForkScenarioRecord) -> None:
        """Publish a pending fork notification to Redis.

        Args:
            fork_id: The fork scenario identifier.
            record: The fork scenario record.
        """
        if self._redis is None:
            return
        payload = msgpack.packb({
            "fork_id": fork_id,
            "parent_hub_id": record.parent_hub_id,
            "divergence_type": record.divergence_type,
            "divergence_reason": record.divergence_reason,
            "detected_at": record.detected_at,
        }, use_bin_type=True)
        await self._redis.publish(CHANNEL_PENDING, payload)

    @with_redis_retry()
    async def _publish_resolved(
        self,
        fork_id: str,
        status: str,
        new_hub_id: str | None = None,
        error: str | None = None,
    ) -> None:
        """Publish a resolved fork notification to Redis.

        Args:
            fork_id: The fork scenario identifier.
            status: Resolution status.
            new_hub_id: New hub ID if spawned.
            error: Error message if failed.
        """
        if self._redis is None:
            return
        payload = msgpack.packb({
            "fork_id": fork_id,
            "status": status,
            "new_hub_id": new_hub_id,
            "error": error,
        }, use_bin_type=True)
        await self._redis.publish(CHANNEL_RESOLVED, payload)

    async def get_fork_scenario(self, fork_id: str) -> ForkScenarioRecord | None:
        """Get a fork scenario by ID.

        Public API for retrieving fork scenario details.

        Args:
            fork_id: The fork scenario identifier.

        Returns:
            The ForkScenarioRecord if found, otherwise None.
        """
        return await self._storage.get_fork_scenario(fork_id)

    async def get_pending_forks(self) -> list[ForkScenarioRecord]:
        """Return all fork scenarios with status 'pending'.

        Delegates to the storage backend.

        Returns:
            List of ForkScenarioRecord with status=='pending'.
        """
        return await self._storage.get_pending_forks()


__all__ = ["ForkHandler", "DivergenceEvent"]
