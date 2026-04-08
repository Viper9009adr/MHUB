"""MCP (Model Context Protocol) manager for tool integration.

Manages MCP server connections and tool discovery for the agentic runtime.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from mcp.client.session import ClientSession
from mcp.client.sse import sse_client

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = ".mcp.json"
MAX_RETRIES = 3
RETRY_DELAY_MS = 1000


class MCPManager:
    """Manager for MCP server connections.

    Handles discovery and invocation of tools exposed via MCP.
    Uses SSE transport to connect to MCP servers.

    Args:
        config_path: Path to the MCP configuration file. Defaults to .mcp.json.
        config: Optional configuration dictionary (overrides config file).
    """

    def __init__(
        self,
        config_path: str | None = None,
        config: dict[str, Any] | None = None,
    ) -> None:
        self._config_path = config_path or DEFAULT_CONFIG_PATH
        self._config = config
        self._servers: dict[str, Any] = {}
        self._sessions: dict[str, ClientSession] = {}
        self._tools: dict[str, dict[str, Any]] = {}
        self._initialized = False

    def _load_config(self) -> dict[str, Any]:
        """Load MCP configuration from file or use provided config.

        Returns:
            Configuration dictionary with mcpServers key.
        """
        if self._config is not None:
            return self._config

        config_file = Path(self._config_path)
        if not config_file.is_absolute():
            # Try to find config relative to current working directory
            config_file = Path.cwd() / self._config_path

        if config_file.exists():
            try:
                with open(config_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except json.JSONDecodeError as exc:
                logger.exception("Failed to parse MCP config file: %s", config_file)
                raise ValueError(f"Invalid JSON in config file: {config_file}") from exc
            except OSError as exc:
                logger.exception("Failed to read MCP config file: %s", config_file)
                raise

        logger.debug("No MCP config file found at %s, using empty config", config_file)
        return {"mcpServers": {}}

    async def _connect_server(self, server_name: str, url: str) -> ClientSession | None:
        """Connect to an MCP server via SSE.

        Args:
            server_name: Name of the server.
            url: SSE URL for the server.

        Returns:
            ClientSession if connected, None on failure.
        """
        for attempt in range(MAX_RETRIES):
            try:
                logger.debug(
                    "Connecting to MCP server %s at %s (attempt %d/%d)",
                    server_name,
                    url,
                    attempt + 1,
                    MAX_RETRIES,
                )
                transport = await sse_client(url)
                session = ClientSession(transport)
                await session.initialize()
                logger.info("Connected to MCP server %s at %s", server_name, url)
                return session
            except Exception as exc:
                logger.exception(
                    "Failed to connect to MCP server %s at %s (attempt %d/%d): %s",
                    server_name,
                    url,
                    attempt + 1,
                    MAX_RETRIES,
                    exc,
                )
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY_MS / 1000.0)

        logger.error("Failed to connect to MCP server %s after %d retries", server_name, MAX_RETRIES)
        return None

    async def initialize(self) -> None:
        """Initialize the MCP manager.

        Discovers available tools from configured MCP servers.
        """
        if self._initialized:
            return

        config = self._load_config()
        mcp_servers = config.get("mcpServers", {})

        if not mcp_servers:
            logger.info("No MCP servers configured")
            self._initialized = True
            return

        # Connect to each configured server
        for server_name, server_config in mcp_servers.items():
            url = server_config.get("url")
            if not url:
                logger.warning("MCP server %s has no URL configured", server_name)
                continue

            session = await self._connect_server(server_name, url)
            if session is not None:
                self._sessions[server_name] = session
                # Discover tools from this server
                await self._discover_server_tools(server_name, session)

        self._initialized = True
        logger.info("MCP manager initialized with %d tools", len(self._tools))

    async def _discover_server_tools(self, server_name: str, session: ClientSession) -> None:
        """Discover tools from a connected MCP server.

        Args:
            server_name: Name of the server.
            session: Connected client session.
        """
        try:
            tools_response = await session.list_tools()
            for tool in tools_response.tools:
                tool_name = tool.name
                self._tools[tool_name] = {
                    "name": tool_name,
                    "server": server_name,
                    "definition": {
                        "name": tool_name,
                        "description": tool.description or "",
                        "inputSchema": tool.inputSchema if hasattr(tool, "inputSchema") else {},
                    },
                }
                logger.debug("Discovered tool %s from server %s", tool_name, server_name)
        except Exception as exc:
            logger.exception(
                "Failed to discover tools from MCP server %s: %s",
                server_name,
                exc,
            )

    async def shutdown(self) -> None:
        """Shutdown the MCP manager.

        Closes all server connections.
        """
        for server_name, session in self._sessions.items():
            try:
                await session.close()
            except Exception as exc:
                logger.exception(
                    "Error closing MCP session for server %s: %s",
                    server_name,
                    exc,
                )

        for server_name, server in self._servers.items():
            try:
                if hasattr(server, "close"):
                    await server.close()
            except Exception as exc:
                logger.exception(
                    "Error closing MCP server %s: %s",
                    server_name,
                    exc,
                )

        self._sessions.clear()
        self._servers.clear()
        self._tools.clear()
        self._initialized = False
        logger.info("MCP manager shutdown complete")

    async def discover_tools(self) -> list[dict[str, Any]]:
        """Discover all available tools from MCP servers.

        Returns:
            List of tool definitions compatible with LLM provider format.
        """
        await self.initialize()
        return [
            {
                "type": "function",
                "function": tool_info["definition"],
            }
            for tool_info in self._tools.values()
        ]

    async def invoke_tool(
        self,
        tool_name: str,
        parameters: dict[str, Any],
    ) -> Any:
        """Invoke a tool via MCP.

        Args:
            tool_name: Name of the tool to invoke.
            parameters: Parameters for the tool.

        Returns:
            Tool execution result.

        Raises:
            ValueError: If tool or server is not found.
        """
        if tool_name not in self._tools:
            raise ValueError(f"Unknown tool: {tool_name}")

        tool_info = self._tools[tool_name]
        server_name = tool_info.get("server")
        if server_name is None:
            raise ValueError(f"Tool {tool_name} has no associated server")

        session = self._sessions.get(server_name)
        if session is None:
            raise ValueError(f"MCP server {server_name} not connected")

        try:
            logger.debug(
                "Invoking tool %s on server %s with parameters: %s",
                tool_name,
                server_name,
                parameters,
            )
            result = await session.call_tool(tool_name, arguments=parameters)
            return result
        except Exception as exc:
            logger.exception(
                "Failed to invoke tool %s on server %s: %s",
                tool_name,
                server_name,
                exc,
            )
            raise

    def register_server(self, name: str, server: Any) -> None:
        """Register an MCP server.

        Args:
            name: Server identifier.
            server: Server instance.
        """
        self._servers[name] = server
        logger.debug("Registered MCP server: %s", name)

    def register_tool(
        self,
        tool_name: str,
        server_name: str,
        definition: dict[str, Any],
    ) -> None:
        """Register a tool from an MCP server.

        Args:
            tool_name: Name of the tool.
            server_name: Server providing the tool.
            definition: Tool definition.
        """
        self._tools[tool_name] = {
            "name": tool_name,
            "server": server_name,
            "definition": definition,
        }
        logger.debug("Registered tool %s from server %s", tool_name, server_name)

    @property
    def tools(self) -> dict[str, dict[str, Any]]:
        """Access registered tools."""
        return self._tools


__all__ = ["MCPManager"]
