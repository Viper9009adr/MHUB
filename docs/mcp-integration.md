# MCP Integration

This document describes the Model Context Protocol (MCP) integration in Meridian-HUB, implemented by **IMP** in the AGENTIC-CLI-V1 slice.

## Overview

MCP (Model Context Protocol) enables discovery and invocation of external tools through a standardized protocol. The `MCPManager` class manages connections to MCP servers via SSE (Server-Sent Events) transport and provides a unified interface for tool discovery and execution.

## Configuration

MCP servers are configured via a JSON file (default: `.mcp.json` in the project root).

### Configuration Format

```json
{
  "mcpServers": {
    "server-name": {
      "url": "http://localhost:8080/sse"
    }
  }
}
```

### Configuration Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `mcpServers` | object | Yes | Map of server names to server configurations |
| `mcpServers.<name>.url` | string | Yes | SSE endpoint URL for the MCP server |

### Example Configuration

```json
{
  "mcpServers": {
    "filesystem": {
      "url": "http://localhost:3001/sse"
    },
    "database": {
      "url": "http://localhost:3002/sse"
    }
  }
}
```

## MCPManager API Reference

The `MCPManager` class is defined in `src/agentic/mcp_manager.py`.

### Constructor

```python
MCPManager(
    config_path: str | None = None,
    config: dict[str, Any] | None = None,
) -> None
```

**Parameters:**
- `config_path`: Path to MCP configuration file. Defaults to `.mcp.json`.
- `config`: Optional configuration dictionary (overrides config file).

### Methods

#### `initialize() -> None`

Initialize the MCP manager. Connects to all configured MCP servers and discovers available tools.

```python
manager = MCPManager()
await manager.initialize()
```

#### `shutdown() -> None`

Shutdown the MCP manager. Closes all server connections and clears internal state.

```python
await manager.shutdown()
```

#### `discover_tools() -> list[dict[str, Any]]`

Discover all available tools from MCP servers. Returns tools in LLM-provider-compatible format.

**Returns:** List of tool definitions with format:
```json
{
  "type": "function",
  "function": {
    "name": "tool_name",
    "description": "Tool description",
    "inputSchema": { ... }
  }
}
```

#### `invoke_tool(tool_name: str, parameters: dict[str, Any]) -> Any`

Invoke a tool via MCP.

**Parameters:**
- `tool_name`: Name of the tool to invoke.
- `parameters`: Parameters for the tool.

**Returns:** Tool execution result.

**Raises:**
- `ValueError`: If tool or server is not found.

#### `register_server(name: str, server: Any) -> None`

Register an MCP server instance.

**Parameters:**
- `name`: Server identifier.
- `server`: Server instance.

#### `register_tool(tool_name: str, server_name: str, definition: dict[str, Any]) -> None`

Register a tool from an MCP server.

**Parameters:**
- `tool_name`: Name of the tool.
- `server_name`: Server providing the tool.
- `definition`: Tool definition.

### Properties

#### `tools -> dict[str, dict[str, Any]]`

Access registered tools. Returns a dictionary mapping tool names to tool information.

## Usage Examples

### Basic Usage

```python
import asyncio
from src.agentic.mcp_manager import MCPManager

async def main():
    manager = MCPManager()
    
    try:
        # Initialize and discover tools
        await manager.initialize()
        
        # List available tools
        tools = await manager.discover_tools()
        for tool in tools:
            print(f"Tool: {tool['function']['name']}")
        
        # Invoke a tool
        result = await manager.invoke_tool(
            "read_file",
            {"path": "/tmp/example.txt"}
        )
        print(f"Result: {result}")
        
    finally:
        await manager.shutdown()

asyncio.run(main())
```

### Custom Configuration Path

```python
manager = MCPManager(config_path="/path/to/custom-mcp.json")
await manager.initialize()
```

### Inline Configuration

```python
config = {
    "mcpServers": {
        "local-tools": {
            "url": "http://localhost:9000/sse"
        }
    }
}
manager = MCPManager(config=config)
await manager.initialize()
```

## Connection Behavior

### Retry Logic

The MCP manager implements retry logic for connection failures:

- **Max retries:** 3 attempts
- **Retry delay:** 1 second between attempts
- **Logging:** All connection failures are logged with full traceback via `logger.exception()`

### Error Handling

All exceptions in MCP operations are logged with full traceback:

- Connection failures trigger retry logic
- Tool discovery failures are logged per-server (does not fail initialization)
- Tool invocation failures raise exceptions to the caller

## Dependencies

The MCP integration requires the `mcp` Python package:

```bash
pip install mcp
```

Key imports:
- `mcp.client.sse.sse_client` — SSE transport client
- `mcp.client.session.ClientSession` — MCP session management

## Integration with Agentic Runtime

The `MCPManager` is designed to integrate with the agentic runtime:

1. **Tool Discovery:** Tools are discovered in LLM-compatible format for direct use with provider APIs
2. **Tool Invocation:** The `invoke_tool` method can be wrapped by `AgentExecutor` for policy-enforced execution
3. **Session Management:** Tools are associated with their source server for routing invocations
