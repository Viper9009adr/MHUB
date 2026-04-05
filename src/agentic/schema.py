"""Pydantic models for agent definitions and tool specifications.

Defines the schema for agent registration and tool configuration
used by the agentic runtime.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ToolDefinition(BaseModel):
    """Definition of a tool available to an agent.

    Attributes:
        name: Unique identifier for the tool.
        description: Human-readable description of what the tool does.
        parameters: JSON schema for tool parameters.
        timeout_ms: Maximum execution time in milliseconds.
        requires_approval: Whether this tool requires human approval.
    """

    name: str
    description: str
    parameters: dict[str, Any] = Field(default_factory=dict)
    timeout_ms: int = Field(default=30000, ge=100, le=300000)
    requires_approval: bool = False


class AgentDefinition(BaseModel):
    """Definition of an agent for registration with the runtime.

    Attributes:
        name: Unique name for the agent.
        version: Semantic version string.
        description: Human-readable description of the agent's purpose.
        tools: List of tools available to this agent.
        max_turns: Maximum number of turns before forced termination.
        timeout_ms: Maximum total execution time in milliseconds.
    """

    name: str
    version: str = "1.0.0"
    description: str = ""
    tools: list[ToolDefinition] = Field(default_factory=list)
    max_turns: int = Field(default=100, ge=1, le=1000)
    timeout_ms: int = Field(default=300000, ge=1000, le=3600000)


class AgentContext(BaseModel):
    """Runtime context for an agent execution.

    Attributes:
        session_id: Unique session identifier.
        hub_id: Hub this session belongs to.
        agent_id: Agent instance identifier.
        run_id: Run identifier for this execution.
        fork_id: Fork context if applicable.
        turn_count: Current turn count.
        status: Current session status.
    """

    session_id: str
    hub_id: str
    agent_id: str
    run_id: str
    fork_id: str | None = None
    turn_count: int = 0
    status: str = "active"


class ToolCallRequest(BaseModel):
    """Request to execute a tool.

    Attributes:
        call_id: Unique identifier for this tool call.
        tool_name: Name of the tool to execute.
        parameters: Parameters to pass to the tool.
        agent_id: Agent making the request.
        session_id: Session context for the call.
    """

    call_id: str
    tool_name: str
    parameters: dict[str, Any] = Field(default_factory=dict)
    agent_id: str
    session_id: str


class ToolCallResult(BaseModel):
    """Result of a tool execution.

    Attributes:
        call_id: Identifier matching the ToolCallRequest.
        success: Whether the tool executed successfully.
        result: The result data if successful.
        error: Error message if failed.
        execution_ms: Time taken to execute in milliseconds.
    """

    call_id: str
    success: bool
    result: Any = None
    error: str | None = None
    execution_ms: int = 0


__all__ = [
    "ToolDefinition",
    "AgentDefinition",
    "AgentContext",
    "ToolCallRequest",
    "ToolCallResult",
]
