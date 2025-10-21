"""Main MCP server implementation using FastAPI and Server-Sent Events (SSE)."""

from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import Tool, TextContent
from fastapi import FastAPI, Depends
from fastapi.responses import Response
from contextlib import asynccontextmanager
import uvicorn
import logging
from typing import Any

from .config import settings
from .jdbc import initialize_connection, get_connection
from .auth import verify_auth
from .tools import discovery, query, profile, metadata, vector

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# MCP Server instance
mcp = Server("calcite-govdata")


# Register MCP Tools
@mcp.list_tools()
async def list_tools() -> list[Tool]:
    """List all available MCP tools."""
    return [
        Tool(
            name="list_schemas",
            description="List all available database schemas in the Calcite data lake",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="list_tables",
            description="List all tables in a specific schema",
            inputSchema={
                "type": "object",
                "properties": {
                    "schema": {"type": "string", "description": "Schema name"},
                    "include_comments": {"type": "boolean", "description": "Include table comments", "default": False}
                },
                "required": ["schema"]
            }
        ),
        Tool(
            name="describe_table",
            description="Get detailed column information for a specific table",
            inputSchema={
                "type": "object",
                "properties": {
                    "schema": {"type": "string", "description": "Schema name"},
                    "table": {"type": "string", "description": "Table name"},
                    "include_comments": {"type": "boolean", "description": "Include column comments", "default": False}
                },
                "required": ["schema", "table"]
            }
        ),
        Tool(
            name="query_data",
            description="Execute a SQL query and return results",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query to execute"},
                    "limit": {"type": "integer", "description": "Maximum rows to return", "default": 100}
                },
                "required": ["sql"]
            }
        ),
        Tool(
            name="sample_table",
            description="Get a sample of rows from a table",
            inputSchema={
                "type": "object",
                "properties": {
                    "schema": {"type": "string", "description": "Schema name"},
                    "table": {"type": "string", "description": "Table name"},
                    "limit": {"type": "integer", "description": "Number of rows to sample", "default": 10}
                },
                "required": ["schema", "table"]
            }
        ),
        Tool(
            name="profile_table",
            description="Get statistical profile of a table",
            inputSchema={
                "type": "object",
                "properties": {
                    "schema": {"type": "string", "description": "Schema name"},
                    "table": {"type": "string", "description": "Table name"},
                    "columns": {"type": "array", "items": {"type": "string"}, "description": "Columns to profile (empty = all)"}
                },
                "required": ["schema", "table"]
            }
        ),
        Tool(
            name="search_metadata",
            description="Search all database metadata for semantic discovery",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query"}
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="semantic_search",
            description="Perform vector similarity search",
            inputSchema={
                "type": "object",
                "properties": {
                    "schema": {"type": "string"},
                    "table": {"type": "string"},
                    "query_text": {"type": "string"},
                    "limit": {"type": "integer", "default": 10},
                    "threshold": {"type": "number", "default": 0.7},
                    "source_table_filter": {"type": "string"},
                    "include_source": {"type": "boolean", "default": False}
                },
                "required": ["schema", "table", "query_text"]
            }
        ),
        Tool(
            name="list_vector_sources",
            description="List source tables for multi-source vector tables",
            inputSchema={
                "type": "object",
                "properties": {
                    "schema": {"type": "string"},
                    "table": {"type": "string"}
                },
                "required": ["schema", "table"]
            }
        ),
    ]


@mcp.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """Execute MCP tool by name."""
    try:
        if name == "list_schemas":
            result = discovery.list_schemas()
        elif name == "list_tables":
            result = discovery.list_tables(**arguments)
        elif name == "describe_table":
            result = discovery.describe_table(**arguments)
        elif name == "query_data":
            result = query.query_data(**arguments)
        elif name == "sample_table":
            result = query.sample_table(**arguments)
        elif name == "profile_table":
            result = profile.profile_table(**arguments)
        elif name == "search_metadata":
            result = metadata.search_metadata(**arguments)
        elif name == "semantic_search":
            result = vector.semantic_search(**arguments)
        elif name == "list_vector_sources":
            result = vector.list_vector_sources(**arguments)
        else:
            raise ValueError(f"Unknown tool: {name}")

        import json
        return [TextContent(type="text", text=json.dumps(result, indent=2))]

    except Exception as e:
        logger.error(f"Error executing tool '{name}': {e}", exc_info=True)
        return [TextContent(type="text", text=f"Error: {str(e)}")]


# FastAPI app for HTTP/SSE transport
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize Calcite connection on startup."""
    logger.info("Starting Govdata MCP Server...")
    logger.info(f"Calcite JAR: {settings.calcite_jar_path}")
    logger.info(f"Calcite Model: {settings.calcite_model_path}")

    try:
        initialize_connection(settings.calcite_jar_path, settings.calcite_model_path)
        logger.info("Calcite connection initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Calcite connection: {e}", exc_info=True)
        raise

    yield

    # Cleanup on shutdown
    try:
        conn = get_connection()
        conn.close()
        logger.info("Calcite connection closed")
    except Exception as e:
        logger.warning(f"Error closing Calcite connection: {e}")


app = FastAPI(
    title="Govdata MCP Server",
    description="Model Context Protocol server for Apache Calcite govdata adapter",
    version="0.1.0",
    lifespan=lifespan
)


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "service": "govdata-mcp-server"}


# SSE endpoint for MCP (with authentication)
sse = SseServerTransport("/messages")

@app.get("/messages")
@app.post("/messages")
async def handle_sse(
    authenticated: bool = Depends(verify_auth)
):
    """Handle MCP Server-Sent Events endpoint."""
    async with sse.connect_sse(
        app.state.request.scope,
        app.state.request.receive,
        app.state.request._send
    ) as streams:
        await mcp.run(
            streams[0], streams[1], mcp.create_initialization_options()
        )


def main():
    """Run the MCP server."""
    uvicorn.run(
        "govdata_mcp.server:app",
        host=settings.server_host,
        port=settings.server_port,
        reload=settings.server_reload,
        log_level=settings.log_level.lower()
    )


if __name__ == "__main__":
    main()