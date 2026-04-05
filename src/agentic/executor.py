"""Agent executor for running agent tasks.

Handles the execution of agent tasks with timeout management,
tool invocation, and result handling.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any

from src.agentic.schema import AgentContext, ToolCallRequest, ToolCallResult
from src.agentic.policy import PolicyEngine
from src.storage.pg import StorageBackend
from src.orc.redis_bridge import RedisBridge

logger = logging.getLogger(__name__)


class AgentExecutor:
    """Executes agent tasks with policy enforcement.

    Manages tool execution, timeout handling, and result collection.

    Args:
        storage: Storage backend for persistence.
        redis: Redis bridge for caching.
        policy: Policy engine for access control.
    """

    def __init__(
        self,
        storage: StorageBackend,
        redis: RedisBridge,
        policy: PolicyEngine,
    ) -> None:
        self._storage = storage
        self._redis = redis
        self._policy = policy
        self._tool_handlers: dict[str, Any] = {}

    def register_tool_handler(self, tool_name: str, handler: Any) -> None:
        """Register a handler for a specific tool.

        Args:
            tool_name: Name of the tool.
            handler: Callable that handles tool execution.
        """
        self._tool_handlers[tool_name] = handler
        logger.debug("Registered handler for tool: %s", tool_name)

	async def execute_tool(
		self,
		request: ToolCallRequest,
		timeout_ms: int = 30000,
	) -> ToolCallResult:
		"""Execute a tool call request.

		Args:
			request: The tool call request to execute.
			timeout_ms: Maximum execution time.

		Returns:
			Result of the tool execution.
		"""
		# Validate request schema
		if not request.call_id or not isinstance(request.call_id, str):
			return ToolCallResult(
				call_id=request.call_id or "",
				success=False,
				error="Invalid call_id in request",
				execution_ms=0,
			)
		if not request.tool_name or not isinstance(request.tool_name, str):
			return ToolCallResult(
				call_id=request.call_id,
				success=False,
				error="Invalid tool_name in request",
				execution_ms=0,
			)
		if request.parameters is None:
			request.parameters = {}
		if not isinstance(request.parameters, dict):
			return ToolCallResult(
				call_id=request.call_id,
				success=False,
				error="parameters must be a dict",
				execution_ms=0,
			)

		start_time = time.time()
		call_id = request.call_id

        # Check policy
        allowed = await self._policy.check(
            agent_id=request.agent_id,
            action=f"tool:{request.tool_name}",
            resource=f"session:{request.session_id}",
        )
        if not allowed:
            return ToolCallResult(
                call_id=call_id,
                success=False,
                error="Policy denied tool execution",
                execution_ms=0,
            )

        # Get handler
        handler = self._tool_handlers.get(request.tool_name)
        if handler is None:
            return ToolCallResult(
                call_id=call_id,
                success=False,
                error=f"No handler registered for tool: {request.tool_name}",
                execution_ms=0,
            )

        # Execute with timeout
        try:
            timeout_s = timeout_ms / 1000.0
            result = await asyncio.wait_for(
                handler(request.parameters),
                timeout=timeout_s,
            )
            execution_ms = int((time.time() - start_time) * 1000)
            return ToolCallResult(
                call_id=call_id,
                success=True,
                result=result,
                execution_ms=execution_ms,
            )
        except asyncio.TimeoutError:
            execution_ms = int((time.time() - start_time) * 1000)
            logger.warning(
                "Tool %s timed out after %dms",
                request.tool_name,
                timeout_ms,
            )
            return ToolCallResult(
                call_id=call_id,
                success=False,
                error=f"Tool execution timed out after {timeout_ms}ms",
                execution_ms=execution_ms,
            )
        except Exception as exc:
            execution_ms = int((time.time() - start_time) * 1000)
            logger.error(
                "Tool %s failed: %s",
                request.tool_name,
                exc,
            )
            return ToolCallResult(
                call_id=call_id,
                success=False,
                error=str(exc),
                execution_ms=execution_ms,
            )

    async def execute_turn(
        self,
        context: AgentContext,
        action: str,
        parameters: dict[str, Any],
    ) -> dict[str, Any]:
        """Execute a single turn of agent execution.

        Args:
            context: Agent session context.
            action: Action to perform.
            parameters: Parameters for the action.

        Returns:
            Result of the turn execution.
        """
        # Increment turn count
        context.turn_count += 1

        # Check policy
        allowed = await self._policy.check(
            agent_id=context.agent_id,
            action=action,
            resource=f"session:{context.session_id}",
        )
        if not allowed:
            return {"success": False, "error": "Policy denied"}

        # Log turn start
        logger.debug(
            "Executing turn %d for agent %s: %s",
            context.turn_count,
            context.agent_id,
            action,
        )

        # Execute based on action type
        result: dict[str, Any] = {"success": True}
        try:
            if action.startswith("tool:"):
                tool_name = action[5:]
                request = ToolCallRequest(
                    call_id=f"call-{uuid.uuid4().hex[:8]}",
                    tool_name=tool_name,
                    parameters=parameters,
                    agent_id=context.agent_id,
                    session_id=context.session_id,
                )
                tool_result = await self.execute_tool(request)
                result["success"] = tool_result.success
                if tool_result.result is not None:
                    result["data"] = tool_result.result
                if tool_result.error:
                    result["error"] = tool_result.error
                result["execution_ms"] = tool_result.execution_ms
            else:
                result["success"] = True
                result["data"] = parameters

        except Exception as exc:
            result["success"] = False
            result["error"] = str(exc)
            logger.error(
                "Turn execution failed for agent %s: %s",
                context.agent_id,
                exc,
            )

        return result


__all__ = ["AgentExecutor"]
