"""Approval gate for human-in-the-loop decisions.

Manages approval requests for actions that require human review,
using Redis for pub/sub notifications and PostgreSQL for persistence.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Any

from src.storage.pg import StorageBackend
from src.storage.models import ApprovalRequestRecord
from src.orc.redis_bridge import RedisBridge, with_redis_retry

logger = logging.getLogger(__name__)

# Redis key patterns
KEY_APPROVAL_PENDING = "approval:pending:{request_id}"  # TTL=3600
CHANNEL_APPROVAL = "meridian:approval:request"
CHANNEL_DECISION = "meridian:approval:decision"


class ApprovalGate:
    """Gate for requesting and managing approvals.

    Provides a workflow for actions that require human approval:
    1. Request approval -> stored in DB + Redis
    2. Wait for decision -> blocks until approved/rejected
    3. Decision made -> update DB + notify via Redis

    Args:
        storage: Storage backend for persistence.
        redis: Redis bridge for pub/sub.
    """

    def __init__(
        self,
        storage: StorageBackend,
        redis: RedisBridge,
    ) -> None:
        self._storage = storage
        self._redis = redis
        self._pending_requests: dict[str, asyncio.Future[bool]] = {}

    def _key_pending(self, request_id: str) -> str:
        """Generate the pending key for a request."""
        return KEY_APPROVAL_PENDING.format(request_id=request_id)

    @with_redis_retry()
    async def request(
        self,
        fork_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Request approval for a fork decision.

        Creates an approval request and publishes it for review.

        Args:
            fork_id: Fork scenario requiring approval.
            metadata: Additional context for the request.

        Returns:
            The request_id for tracking.
        """
        request_id = f"approval-{uuid.uuid4().hex[:8]}"
        created_at = int(time.time() * 1000)

        # Create record
        record = ApprovalRequestRecord(
            request_id=request_id,
            fork_id=fork_id,
            status="pending",
            created_at=created_at,
        )
        await self._storage.insert_approval_request(record)

        # Store in Redis with TTL
        client = self._redis.client
        if client is None:
            raise ConnectionError("Redis client not connected")

        pending_key = self._key_pending(request_id)
        payload = {
            "request_id": request_id,
            "fork_id": fork_id,
            "created_at": created_at,
            "metadata": metadata or {},
        }
        await client.set(
            pending_key,
            json.dumps(payload),
            ex=3600,  # 1 hour TTL
        )

        # Publish request notification
        await self._redis.publish_json(CHANNEL_APPROVAL, payload)

        logger.info("Created approval request %s for fork %s", request_id, fork_id)
        return request_id

	@with_redis_retry()
	async def decide(
		self,
		request_id: str,
		decision: str,
	) -> bool:
		"""Make a decision on an approval request.

		Args:
			request_id: Request to decide on.
			decision: Either 'approve' or 'reject'.

		Returns:
			True if the decision was recorded.
		"""
		# Validate request_id
		if not request_id or not isinstance(request_id, str):
			raise ValueError("request_id must be a non-empty string")
		if not request_id.startswith("approval-"):
			raise ValueError(f"Invalid request_id format: {request_id}")

		if decision not in ("approve", "reject"):
			raise ValueError(f"Invalid decision: {decision}")

        decided_at = int(time.time() * 1000)
        status = "approved" if decision == "approve" else "rejected"

        # Update database
        updated = await self._storage.update_approval_decision(
            request_id=request_id,
            decision=decision,
            decided_at=decided_at,
            status=status,
        )
        if not updated:
            logger.warning("Approval request %s not found", request_id)
            return False

        # Update Redis
        client = self._redis.client
        if client is None:
            raise ConnectionError("Redis client not connected")

        pending_key = self._key_pending(request_id)
        await client.delete(pending_key)

        # Publish decision notification
        await self._redis.publish_json(
            CHANNEL_DECISION,
            {
                "request_id": request_id,
                "decision": decision,
                "decided_at": decided_at,
            },
        )

        # Resolve any waiting futures
        if request_id in self._pending_requests:
            future = self._pending_requests.pop(request_id)
            if not future.done():
                future.set_result(decision == "approve")

        logger.info("Decision %s recorded for request %s", decision, request_id)
        return True

    async def wait(
        self,
        request_id: str,
        timeout_ms: int = 60000,
    ) -> bool:
        """Wait for a decision on an approval request.

        Blocks until a decision is made or timeout expires.

        Args:
            request_id: Request to wait for.
            timeout_ms: Maximum time to wait in milliseconds.

        Returns:
            True if approved, False if rejected or timed out.
        """
        # Check if already decided
        record = await self._storage.get_approval_request(request_id)
        if record is not None and record.status in ("approved", "rejected"):
            return record.status == "approved"

        # Create future for waiting
        future: asyncio.Future[bool] = asyncio.get_event_loop().create_future()
        self._pending_requests[request_id] = future

        try:
            timeout_s = timeout_ms / 1000.0
            return await asyncio.wait_for(future, timeout=timeout_s)
        except asyncio.TimeoutError:
            self._pending_requests.pop(request_id, None)
            logger.warning("Approval request %s timed out", request_id)
            return False

    @with_redis_retry()
    async def get_pending(self) -> list[ApprovalRequestRecord]:
        """Get all pending approval requests.

        Returns:
            List of pending approval requests.
        """
        return await self._storage.get_pending_approvals()


__all__ = ["ApprovalGate"]
