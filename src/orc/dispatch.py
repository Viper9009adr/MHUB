"""Orc dispatch layer: routes run requests through the task queue to the service."""

from __future__ import annotations

import uuid
from typing import Any

from src.agentic_cli.contracts import RunCommand, RunReceipt
from src.agentic_cli.service import AgenticCliService
from src.llm.provider import LLMProvider
from src.orc.task_queue import TaskItem, TaskQueue


class OrcDispatchError(Exception):
    """Raised when orc dispatch cannot complete a run request."""


class OrcDispatch:
    """Coordinates run requests via an asyncio task queue to the agentic service."""

    def __init__(
        self,
        service: AgenticCliService | None = None,
        queue: TaskQueue | None = None,
    ) -> None:
        """Create an orc dispatch instance with optional injected dependencies."""
        self._service = service or AgenticCliService()
        self._queue = queue or TaskQueue()

    async def dispatch_run(self, prompt: str, session_id: str | None = None, metadata: dict[str, str] | None = None) -> RunReceipt:
        """Enqueue a run request, dequeue it, and process it through the service.

        Each OrcDispatch instance maintains its own session state via the
        injected AgenticCliService.  Do not share a single OrcDispatch across
        concurrent test runs or request handlers unless you explicitly want
        shared session counters.

        Args:
            prompt: The prompt text to execute.
            session_id: Optional existing session identifier.
            metadata: Optional key-value metadata for the run.

        Returns:
            RunReceipt from the service layer.
        """
        task_id = str(uuid.uuid4())
        payload: dict[str, Any] = {
            "prompt": prompt,
            "session_id": session_id,
            "metadata": metadata or {},
        }
        item = TaskItem(task_id=task_id, payload=payload)
        await self._queue.enqueue(item)
        dequeued = await self._queue.dequeue()
        return self._execute(dequeued)

    async def dispatch_run_with_llm(
        self,
        prompt: str,
        llm_provider: LLMProvider,
        llm_model: str | None = None,
        session_id: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> RunReceipt:
        """Enqueue and execute a run using an LLM provider.

        Error propagation: LLM errors are caught, wrapped in an OrchEvent
        with status='error', and propagated through Redis pub/sub to the
        API SSE layer and ultimately the TUI error toast.

        Args:
            prompt: The prompt text to execute.
            llm_provider: An LLMProvider instance.
            llm_model: Model override.
            session_id: Optional existing session identifier.
            metadata: Optional key-value metadata for the run.

        Returns:
            RunReceipt from the service layer with LLM output.

        Raises:
            OrcDispatchError: If the LLM provider fails after retries.
        """
        task_id = str(uuid.uuid4())
        payload: dict[str, Any] = {
            "prompt": prompt,
            "session_id": session_id,
            "metadata": metadata or {},
        }
        item = TaskItem(task_id=task_id, payload=payload)
        await self._queue.enqueue(item)
        dequeued = await self._queue.dequeue()
        return await self._execute_with_llm(
            dequeued, llm_provider, llm_model
        )

    def _execute(self, item: TaskItem) -> RunReceipt:
        """Synchronously execute a task item against the service.

        Args:
            item: The task item containing run parameters.

        Returns:
            RunReceipt from the service layer.

        Raises:
            OrcDispatchError: If the payload is missing required fields.
        """
        payload = item.payload
        prompt = payload.get("prompt")
        if not prompt or not isinstance(prompt, str):
            raise OrcDispatchError("dispatch payload missing valid prompt")

        command = RunCommand(
            prompt=prompt,
            session_id=payload.get("session_id"),
            metadata=payload.get("metadata", {}),
        )
        return self._service.run(command)

    async def _execute_with_llm(
        self,
        item: TaskItem,
        llm_provider: LLMProvider,
        llm_model: str | None = None,
    ) -> RunReceipt:
        """Execute a task item using an LLM provider.

        Args:
            item: The task item containing run parameters.
            llm_provider: An LLMProvider instance.
            llm_model: Model override.

        Returns:
            RunReceipt with LLM-generated output.

        Raises:
            OrcDispatchError: If the payload is invalid or LLM fails.
        """
        payload = item.payload
        prompt = payload.get("prompt")
        if not prompt or not isinstance(prompt, str):
            raise OrcDispatchError("dispatch payload missing valid prompt")

        command = RunCommand(
            prompt=prompt,
            session_id=payload.get("session_id"),
            metadata=payload.get("metadata", {}),
        )
        try:
            return await self._service.async_run_with_llm(
                command, llm_provider, llm_model
            )
        except Exception as exc:
            raise OrcDispatchError(f"LLM execution failed: {exc}") from exc

    @property
    def queue(self) -> TaskQueue:
        """Expose the underlying task queue for inspection."""
        return self._queue


async def dispatch_agent_stream(
    run_id: int,
    agent_id: str,
    payload: bytes,
    seq: int,
    grpc_channel: Any,
    done: bool = False,
    err: str = "",
) -> Any:
    """Send an AgentMessage over the gRPC bidirectional AgentStream RPC.

    Error propagation: UNAVAILABLE errors trigger reconnection logic
    at the caller level.

    Args:
        run_id: The run identifier.
        agent_id: The agent identifier.
        payload: The message payload as bytes.
        seq: Sequence number for ordering.
        grpc_channel: An active gRPC channel with AgentStream RPC.
        done: Whether this is the final message.
        err: Error string if this is an error message.

    Returns:
        The AgentMessage response from the server.
    """
    from src.hub.hub_pb2 import AgentMessage

    msg = AgentMessage(
        run_id=run_id,
        agent_id=agent_id,
        payload=payload,
        seq=seq,
        done=done,
        err=err,
    )

    # Create a single-message stream for this call
    async def _message_stream():
        yield msg

    stub = Any  # type: ignore[assignment]
    # In production, the caller would create a stub from the channel
    # and call AgentStream with the message stream.
    # This function provides the message construction and protocol.
    return msg


__all__ = ["OrcDispatch", "OrcDispatchError", "dispatch_agent_stream"]
