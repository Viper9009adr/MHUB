"""Agentic runtime package for Meridian HUB.

Provides the core components for agent lifecycle management,
execution, and coordination.
"""

from __future__ import annotations

from src.agentic.schema import (
    ToolDefinition,
    AgentDefinition,
    AgentContext,
    ToolCallRequest,
    ToolCallResult,
)
from src.agentic.runtime import AgentRuntime
from src.agentic.registry import AgentRegistry
from src.agentic.policy import PolicyEngine
from src.agentic.executor import AgentExecutor
from src.agentic.approval import ApprovalGate
from src.agentic.context_store import ContextStore
from src.agentic.audit import AuditLogger
from src.agentic.mcp_manager import MCPManager
from src.agentic.shutdown import ShutdownHandler, graceful_shutdown
from src.agentic.stream_router import AgenticStreamRouter, BackpressureError

__all__ = [
    # Schema
    "ToolDefinition",
    "AgentDefinition",
    "AgentContext",
    "ToolCallRequest",
    "ToolCallResult",
    # Runtime
    "AgentRuntime",
    # Components
    "AgentRegistry",
    "PolicyEngine",
    "AgentExecutor",
    "ApprovalGate",
    "ContextStore",
    "AuditLogger",
    "MCPManager",
    # Shutdown
    "ShutdownHandler",
    "graceful_shutdown",
    # Stream routing
    "AgenticStreamRouter",
    "BackpressureError",
]
