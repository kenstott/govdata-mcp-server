"""Main MCP server implementation supporting both stdio and HTTP/SSE transports."""

from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent, Prompt, Resource, PromptMessage, GetPromptResult, ReadResourceResult, TextResourceContents
from fastapi import FastAPI, Depends, Request
from fastapi.responses import Response
from contextlib import asynccontextmanager
import uvicorn
import logging
from typing import Any
import os
import sys

from .config import settings
from .jdbc import initialize_connection, get_connection
from .auth import verify_auth, headers_authenticated
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
            description="""Execute a SQL query against the Calcite data lake and return results.

IMPORTANT SQL SYNTAX RULES (lex=ORACLE mode):
- Use DOUBLE QUOTES for identifiers (column/table names/aliases), not single quotes
- Single quotes are ONLY for string literals
- ALWAYS quote SQL reserved words when used as column/table names OR aliases

Common reserved words that MUST be quoted as identifiers:
  Temporal: "year", "month", "day", "hour", "minute", "second", "date", "time", "timestamp", "interval", "zone"
  System: "user", "group", "role", "session", "current", "level", "partition", "schema", "database"
  Clauses: "order", "by", "group", "having", "where", "limit", "offset", "fetch"
  DDL: "table", "column", "index", "view", "constraint", "key", "foreign", "primary"
  Functions: "count", "sum", "avg", "min", "max", "rank", "dense_rank", "lead", "lag"
  Data: "value", "values", "row", "comment", "status", "type", "name", "option", "position"
  Oracle-specific: "rownum", "rowid", "connect", "start"

Examples:
  ✓ SELECT "year", "month" FROM census.population WHERE "year" > 2020
  ✗ SELECT year, month FROM census.population WHERE year > 2020
  ✓ SELECT * FROM sec.filings WHERE "date" BETWEEN '2020-01-01' AND '2020-12-31'
  ✗ SELECT * FROM sec.filings WHERE date BETWEEN "2020-01-01" AND "2020-12-31"
  ✓ SELECT DISTINCT "area_type", COUNT(*) as "count" FROM econ.regional_employment GROUP BY "area_type"
  ✗ SELECT DISTINCT "area_type", COUNT(*) as count FROM econ.regional_employment GROUP BY "area_type"

When in doubt, quote all identifiers (including aliases) to avoid syntax errors.""",
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


# Prompts - Pre-defined prompt templates
@mcp.list_prompts()
async def list_prompts() -> list[Prompt]:
    """List available prompt templates."""
    return [
        Prompt(
            name="analyze_economic_trends",
            description="Analyze economic indicators and trends over time",
            arguments=[
                {"name": "indicators", "description": "Comma-separated list of economic indicators (e.g., UNRATE, DGS10, CPIAUCSL)", "required": True},
                {"name": "start_year", "description": "Start year for analysis", "required": False},
                {"name": "end_year", "description": "End year for analysis", "required": False}
            ]
        ),
        Prompt(
            name="compare_sec_filings",
            description="Compare SEC filings across companies or time periods",
            arguments=[
                {"name": "ciks", "description": "Comma-separated list of CIK numbers", "required": True},
                {"name": "filing_type", "description": "Filing type (10-K, 10-Q, 8-K, etc.)", "required": True},
                {"name": "year", "description": "Year to analyze", "required": False}
            ]
        ),
        Prompt(
            name="explore_schema",
            description="Get an overview of a schema's tables and structure",
            arguments=[
                {"name": "schema", "description": "Schema name (sec, econ, census, geo)", "required": True}
            ]
        ),
        Prompt(
            name="query_with_best_practices",
            description="Template for writing SQL queries with proper quoting for reserved words",
            arguments=[
                {"name": "table_path", "description": "Full table path (schema.table)", "required": True},
                {"name": "columns", "description": "Columns to select (will be properly quoted)", "required": False}
            ]
        )
    ]


@mcp.get_prompt()
async def get_prompt(name: str, arguments: dict[str, str] | None = None) -> GetPromptResult:
    """Get a specific prompt template with filled-in arguments."""
    args = arguments or {}

    if name == "analyze_economic_trends":
        indicators = args.get("indicators", "UNRATE, DGS10, CPIAUCSL")
        start_year = args.get("start_year", "2020")
        end_year = args.get("end_year", "2024")

        prompt_text = f"""Analyze economic trends for the following indicators: {indicators}

Time period: {start_year} to {end_year}

Please:
1. Query the econ schema to get data for these indicators
2. Remember to quote reserved words like "year", "date", "value" in your SQL queries
3. Calculate year-over-year changes
4. Identify any significant trends or correlations
5. Provide visualizations or summary statistics

Example query structure:
SELECT "year", "series_id", "value"
FROM econ.fred_series
WHERE "series_id" IN ('{indicators.replace(", ", "', '")}')
  AND "year" BETWEEN {start_year} AND {end_year}
ORDER BY "year", "series_id"
"""

        return GetPromptResult(
            description=f"Analysis of economic indicators: {indicators}",
            messages=[
                PromptMessage(
                    role="user",
                    content=TextContent(type="text", text=prompt_text)
                )
            ]
        )

    elif name == "compare_sec_filings":
        ciks = args.get("ciks", "")
        filing_type = args.get("filing_type", "10-K")
        year = args.get("year", "2023")

        prompt_text = f"""Compare SEC {filing_type} filings for CIKs: {ciks} in year {year}

Please:
1. List all tables in the sec schema using the list_tables tool
2. Query filing metadata and text for these companies
3. Remember to quote reserved words like "year", "date", "type" in SQL queries
4. Compare key metrics, filing dates, and content themes
5. Identify similarities and differences across companies

Example query structure:
SELECT "cik", "filing_date", "form_type", "accession_number"
FROM sec.filings
WHERE "cik" IN ('{ciks.replace(", ", "', '")}')
  AND "form_type" = '{filing_type}'
  AND "year" = {year}
ORDER BY "filing_date"
"""

        return GetPromptResult(
            description=f"Comparison of {filing_type} filings for {ciks}",
            messages=[
                PromptMessage(
                    role="user",
                    content=TextContent(type="text", text=prompt_text)
                )
            ]
        )

    elif name == "explore_schema":
        schema = args.get("schema", "econ")

        prompt_text = f"""Explore the {schema} schema structure

Please:
1. Use list_tables to get all tables in the {schema} schema
2. For key tables, use describe_table to see column structure
3. Use sample_table to preview data from interesting tables
4. Summarize what data is available and how it's organized
5. Suggest interesting queries or analyses that could be performed

Remember to quote reserved words in any SQL queries you write!
"""

        return GetPromptResult(
            description=f"Exploration of {schema} schema",
            messages=[
                PromptMessage(
                    role="user",
                    content=TextContent(type="text", text=prompt_text)
                )
            ]
        )

    elif name == "query_with_best_practices":
        table_path = args.get("table_path", "schema.table")
        columns = args.get("columns", "*")

        # Quote column names if provided
        if columns != "*":
            quoted_columns = ', '.join([f'"{col.strip()}"' for col in columns.split(",")])
        else:
            quoted_columns = "*"

        prompt_text = f"""Best practices for querying {table_path}

IMPORTANT: When writing SQL queries for this Calcite server (lex=ORACLE mode):

1. Use DOUBLE QUOTES for identifiers (column/table names/aliases)
2. Use SINGLE QUOTES for string literals
3. Always quote SQL reserved words when used as identifiers

Common reserved words to quote:
- Temporal: "year", "month", "day", "date", "time"
- Aggregates: "count", "sum", "avg", "min", "max"
- System: "user", "group", "value", "type", "name"

Example query for {table_path}:
SELECT {quoted_columns}
FROM {table_path}
LIMIT 10

Now please write your actual query following these rules!
"""

        return GetPromptResult(
            description=f"Query template with SQL best practices for {table_path}",
            messages=[
                PromptMessage(
                    role="user",
                    content=TextContent(type="text", text=prompt_text)
                )
            ]
        )

    else:
        raise ValueError(f"Unknown prompt: {name}")


# Resources - Exposed data/metadata
@mcp.list_resources()
async def list_resources() -> list[Resource]:
    """List available resources."""
    try:
        # Get list of schemas
        schemas_result = discovery.list_schemas()
        schemas = schemas_result.get("schemas", [])

        resources = [
            Resource(
                uri="govdata://schemas",
                name="All Schemas",
                description="Complete list of all available schemas in the govdata Calcite instance",
                mimeType="application/json"
            ),
            Resource(
                uri="govdata://sql-best-practices",
                name="SQL Best Practices",
                description="Guide for writing SQL queries with proper identifier quoting (lex=ORACLE mode)",
                mimeType="text/markdown"
            )
        ]

        # Add a resource for each schema's table list
        for schema in schemas:
            resources.append(
                Resource(
                    uri=f"govdata://schemas/{schema}/tables",
                    name=f"{schema.upper()} Schema Tables",
                    description=f"List of all tables in the {schema} schema with metadata",
                    mimeType="application/json"
                )
            )

        return resources

    except Exception as e:
        logger.error(f"Error listing resources: {e}")
        return []


@mcp.read_resource()
async def read_resource(uri: str) -> ReadResourceResult:
    """Read a specific resource."""
    import json

    if uri == "govdata://schemas":
        # Return all schemas
        result = discovery.list_schemas()
        return ReadResourceResult(
            contents=[
                TextResourceContents(
                    uri=uri,
                    text=json.dumps(result, indent=2),
                    mimeType="application/json"
                )
            ]
        )

    elif uri == "govdata://sql-best-practices":
        # Return SQL best practices guide
        guide = """# SQL Best Practices for Govdata MCP Server

## Identifier Quoting (lex=ORACLE mode)

This Calcite server uses `lex=ORACLE` mode, which requires specific syntax:

### Rule 1: Use Double Quotes for Identifiers
- Column names: `"year"`, `"month"`, `"value"`
- Table names: `"TABLES"`, `"COLUMNS"`
- Aliases: `COUNT(*) as "count"`

### Rule 2: Use Single Quotes for String Literals
- String values: `'2024-01-01'`, `'10-K'`, `'AAPL'`

### Rule 3: Always Quote Reserved Words

**Temporal:** "year", "month", "day", "hour", "minute", "second", "date", "time", "timestamp"
**Aggregates:** "count", "sum", "avg", "min", "max", "rank"
**System:** "user", "group", "role", "session", "level", "partition", "value", "type", "name"

## Examples

✓ **Correct:**
```sql
SELECT "year", "month", COUNT(*) as "count"
FROM econ.fred_series
WHERE "year" > 2020
GROUP BY "year", "month"
```

✗ **Incorrect:**
```sql
SELECT year, month, COUNT(*) as count
FROM econ.fred_series
WHERE year > 2020
GROUP BY year, month
```

## Quick Reference

| Context | Syntax | Example |
|---------|--------|---------|
| Column name | `"column"` | `SELECT "year"` |
| String literal | `'text'` | `WHERE "name" = 'Apple'` |
| Alias | `AS "alias"` | `COUNT(*) AS "total"` |
| Reserved word | `"word"` | `WHERE "date" = '2024-01-01'` |
"""
        return ReadResourceResult(
            contents=[
                TextResourceContents(
                    uri=uri,
                    text=guide,
                    mimeType="text/markdown"
                )
            ]
        )

    elif uri.startswith("govdata://schemas/") and uri.endswith("/tables"):
        # Extract schema name from URI: govdata://schemas/{schema}/tables
        schema = uri.split("/")[3]

        # Get tables for this schema
        result = discovery.list_tables(schema=schema, include_comments=True)
        return ReadResourceResult(
            contents=[
                TextResourceContents(
                    uri=uri,
                    text=json.dumps(result, indent=2),
                    mimeType="application/json"
                )
            ]
        )

    else:
        raise ValueError(f"Unknown resource URI: {uri}")


# FastAPI app for HTTP/SSE transport
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize Calcite connection on startup."""
    logger.info("Starting Govdata MCP Server...")
    logger.info(f"Calcite JAR: {settings.calcite_jar_path}")
    logger.info(f"Calcite Model: {settings.calcite_model_path}")

    # Auth configuration sanity hints
    if os.environ.get("OIDC_ISSUER") and not settings.oidc_issuer_url:
        logger.warning(
            "Detected OIDC_ISSUER in environment but OIDC_ISSUER_URL is not set. Did you mean OIDC_ISSUER_URL?"
        )
    if settings.oidc_enabled and settings.auth_allow_local_jwt_fallback:
        logger.warning(
            "OIDC is enabled and local JWT fallback is also enabled. This will accept both provider and HS256 tokens."
        )

    # Authentication configuration summary
    try:
        if settings.oidc_enabled:
            if not settings.oidc_issuer_url or not settings.oidc_audience:
                logger.warning(
                    "OIDC is enabled but OIDC_ISSUER_URL or OIDC_AUDIENCE is not set. Tokens will fail to validate."
                )
            else:
                jwks_source = (
                    settings.oidc_jwks_url if settings.oidc_jwks_url else "auto-discovery"
                )
                logger.info(
                    "Auth: OIDC enabled (issuer=%s, audience=%s, jwks=%s, local_jwt_fallback=%s)",
                    settings.oidc_issuer_url,
                    settings.oidc_audience,
                    jwks_source,
                    "enabled" if settings.auth_allow_local_jwt_fallback else "disabled",
                )
        else:
            if settings.jwt_secret_key and settings.jwt_algorithm:
                logger.info(
                    "Auth: OIDC disabled. Accepting API keys and local JWT (%s).",
                    settings.jwt_algorithm,
                )
            else:
                logger.info("Auth: OIDC disabled. Accepting API keys only.")
    except Exception as e:
        logger.warning(f"Unable to log auth configuration summary: {e}")

    try:
        initialize_connection(settings.calcite_jar_path, settings.calcite_model_path)
        logger.info("Calcite connection initialized successfully")
        # Log exposed endpoints (primary and alias) for clarity
        logger.info("Endpoints: /messages (primary), /sse (alias)")
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
    lifespan=lifespan,
    redirect_slashes=False  # Disable automatic trailing slash redirects
)


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "service": "govdata-mcp-server"}


# SSE endpoint for MCP (with authentication)
sse = SseServerTransport("")

# ASGI app to handle SSE with proper access to scope/receive/send and manual auth
async def messages_asgi(scope, receive, send):
    start_ts = None
    try:
        start_ts = __import__("time").time()
    except Exception:
        pass

    if scope.get("type") != "http":
        # Only handle HTTP
        try:
            logger.debug("/messages received non-HTTP scope: %s", scope.get("type"))
        except Exception:
            pass
        await send({"type": "http.response.start", "status": 404, "headers": []})
        await send({"type": "http.response.body", "body": b"Not Found"})
        return

    # Basic request metadata
    method = scope.get("method")
    path = scope.get("path")
    query = (scope.get("query_string") or b"").decode(errors="ignore")
    client = scope.get("client") or (None, None)
    client_ip, client_port = (client + (None, None))[:2] if isinstance(client, tuple) else (None, None)

    # Extract headers into a dict and authenticate (API key or JWT/OIDC)
    headers = {k.decode().lower(): v.decode(errors="ignore") for k, v in scope.get("headers", [])}

    # DEBUG: Log all received headers (temporarily for troubleshooting)
    try:
        logger.debug("[SSE] All received headers: %s", list(headers.keys()))
    except Exception:
        pass

    # Derive auth presence for logging without leaking secrets
    api_key_present = "x-api-key" in headers
    auth_header = headers.get("authorization")
    bearer_present = bool(auth_header and auth_header.lower().startswith("bearer "))

    # Masked snippets for debug visibility (do not log full secrets)
    def _mask(v: str) -> str:
        if not v:
            return ""
        if len(v) <= 6:
            return "***"
        return v[:4] + "…" + v[-2:]

    masked_api_key = _mask(headers.get("x-api-key", "")) if api_key_present else None
    masked_bearer = _mask(auth_header.split(" ", 1)[1]) if bearer_present else None

    try:
        logger.debug(
            "[SSE] /messages request: method=%s path=%s?%s client=%s:%s api_key_present=%s bearer_present=%s api_key=%s bearer=%s",
            method,
            path,
            query,
            client_ip,
            client_port,
            api_key_present,
            bearer_present,
            masked_api_key,
            masked_bearer,
        )
    except Exception:
        pass

    # Attempt authentication
    authed = headers_authenticated(headers)

    if not authed:
        try:
            reason = (
                "missing auth headers" if (not api_key_present and not bearer_present) else
                ("invalid API key" if api_key_present and not bearer_present else "invalid bearer token")
            )
            logger.info("[SSE] /messages auth failed for %s:%s (%s)", client_ip, client_port, reason)
        except Exception:
            pass
        body = b'{"detail":"Invalid authentication. Provide either X-API-Key header or JWT Bearer token."}'
        await send({
            "type": "http.response.start",
            "status": 401,
            "headers": [
                (b"content-type", b"application/json"),
                (b"www-authenticate", b"Bearer"),
            ],
        })
        await send({"type": "http.response.body", "body": body})
        return

    # Authenticated: proceed with SSE connection
    try:
        logger.info(
            "[SSE] /messages auth succeeded for %s:%s (method=%s, path=%s, query=%s, mode=%s)",
            client_ip,
            client_port,
            method,
            path,
            query,
            "API Key" if api_key_present else ("Bearer" if bearer_present else "unknown"),
        )
    except Exception:
        pass

    # Normalize path to avoid trailing slash in announced endpoint (e.g., '/messages?session_id=…')
    try:
        normalized_path = path if path == "/" else path.rstrip("/")
        if normalized_path != path:
            logger.debug("[SSE] normalizing path for transport: original='%s' -> normalized='%s'", path, normalized_path)
        # Prepare a shallow copy of the scope with normalized path
        new_scope = dict(scope)
        new_scope["path"] = normalized_path
    except Exception:
        # Fallback to original scope if anything goes wrong
        new_scope = scope

    # Handle Streamable HTTP: POST requests without session_id
    # This supports mcp-remote which uses StreamableHTTPClientTransport
    try:
        if method == "POST":
            # Read entire request body
            total = bytearray()
            while True:
                msg = await receive()
                if msg.get("type") == "http.request":
                    chunk = msg.get("body") or b""
                    total.extend(chunk)
                    if not msg.get("more_body"):
                        break
                elif msg.get("type") == "http.disconnect":
                    break

            import json as _json
            try:
                payload = _json.loads(total.decode("utf-8")) if total else {}
            except Exception:
                payload = {}

            method_name = payload.get("method") if isinstance(payload, dict) else None
            req_id = payload.get("id") if isinstance(payload, dict) else None

            logger.debug("[HTTP] Received JSON-RPC request: method=%s id=%s", method_name, req_id)

            # Handle different message types
            if method_name == "initialize":
                logger.info("[HTTP] Handling initialize request")
                result = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {
                            "tools": {},
                            "prompts": {},
                            "resources": {}
                        },
                        "serverInfo": {"name": "calcite-govdata", "version": "0.1.0"},
                    },
                }
                body = _json.dumps(result).encode("utf-8")
                await send({
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"cache-control", b"no-store"),
                    ],
                })
                await send({"type": "http.response.body", "body": body})
                return

            elif method_name == "tools/list":
                logger.info("[HTTP] Handling tools/list request")
                tools_list = await list_tools()
                result = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "tools": [
                            {
                                "name": tool.name,
                                "description": tool.description,
                                "inputSchema": tool.inputSchema
                            }
                            for tool in tools_list
                        ]
                    },
                }
                body = _json.dumps(result).encode("utf-8")
                await send({
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"cache-control", b"no-store"),
                    ],
                })
                await send({"type": "http.response.body", "body": body})
                return

            elif method_name == "tools/call":
                logger.info("[HTTP] Handling tools/call request")
                params = payload.get("params", {})
                tool_name = params.get("name")
                tool_arguments = params.get("arguments", {})

                # Call the tool
                tool_result = await call_tool(tool_name, tool_arguments)

                result = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": item.type, "text": item.text} for item in tool_result]
                    },
                }
                body = _json.dumps(result).encode("utf-8")
                await send({
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"cache-control", b"no-store"),
                    ],
                })
                await send({"type": "http.response.body", "body": body})
                return

            elif method_name == "prompts/list":
                logger.info("[HTTP] Handling prompts/list request")
                prompts_list = await list_prompts()
                result = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "prompts": [
                            {
                                "name": prompt.name,
                                "description": prompt.description,
                                "arguments": prompt.arguments
                            }
                            for prompt in prompts_list
                        ]
                    },
                }
                body = _json.dumps(result).encode("utf-8")
                await send({
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"cache-control", b"no-store"),
                    ],
                })
                await send({"type": "http.response.body", "body": body})
                return

            elif method_name == "prompts/get":
                logger.info("[HTTP] Handling prompts/get request")
                params = payload.get("params", {})
                prompt_name = params.get("name")
                prompt_arguments = params.get("arguments", {})

                # Get the prompt
                prompt_result = await get_prompt(prompt_name, prompt_arguments)

                result = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "description": prompt_result.description,
                        "messages": [
                            {
                                "role": msg.role,
                                "content": {
                                    "type": msg.content.type,
                                    "text": msg.content.text
                                }
                            }
                            for msg in prompt_result.messages
                        ]
                    },
                }
                body = _json.dumps(result).encode("utf-8")
                await send({
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"cache-control", b"no-store"),
                    ],
                })
                await send({"type": "http.response.body", "body": body})
                return

            elif method_name == "resources/list":
                logger.info("[HTTP] Handling resources/list request")
                resources_list = await list_resources()
                result = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "resources": [
                            {
                                "uri": resource.uri,
                                "name": resource.name,
                                "description": resource.description,
                                "mimeType": resource.mimeType
                            }
                            for resource in resources_list
                        ]
                    },
                }
                body = _json.dumps(result).encode("utf-8")
                await send({
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"cache-control", b"no-store"),
                    ],
                })
                await send({"type": "http.response.body", "body": body})
                return

            elif method_name == "resources/read":
                logger.info("[HTTP] Handling resources/read request")
                params = payload.get("params", {})
                resource_uri = params.get("uri")

                # Read the resource
                read_result = await read_resource(resource_uri)

                result = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "contents": [
                            {
                                "uri": str(content.uri),
                                "text": content.text,
                                "mimeType": content.mimeType
                            }
                            for content in read_result.contents
                        ]
                    },
                }
                body = _json.dumps(result).encode("utf-8")
                await send({
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"cache-control", b"no-store"),
                    ],
                })
                await send({"type": "http.response.body", "body": body})
                return

            elif method_name and method_name.startswith("notifications/"):
                # Handle notifications (no response needed)
                logger.debug("[HTTP] Received notification: %s", method_name)
                await send({
                    "type": "http.response.start",
                    "status": 204,
                    "headers": [],
                })
                await send({"type": "http.response.body", "body": b""})
                return

            else:
                logger.warning("[HTTP] Unknown method: %s", method_name)
                error_response = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {
                        "code": -32601,
                        "message": f"Method not found: {method_name}",
                    },
                }
                body = _json.dumps(error_response).encode("utf-8")
                await send({
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"cache-control", b"no-store"),
                    ],
                })
                await send({"type": "http.response.body", "body": body})
                return
    except Exception as e:
        logger.error("[HTTP] Error handling request: %s", e, exc_info=True)
        error_response = {
            "jsonrpc": "2.0",
            "id": None,
            "error": {
                "code": -32603,
                "message": f"Internal error: {str(e)}",
            },
        }
        body = _json.dumps(error_response).encode("utf-8")
        await send({
            "type": "http.response.start",
            "status": 500,
            "headers": [
                (b"content-type", b"application/json"),
            ],
        })
        await send({"type": "http.response.body", "body": body})
        return

    # Wrap ASGI receive/send to add deep diagnostics of incoming POST bodies and disconnect reasons
    last_msg_info = {"id": None, "method": None, "size": 0}
    body_accumulator = bytearray()

    async def logging_receive():
        msg = await receive()
        try:
            mtype = msg.get("type")
            if mtype == "http.request":
                chunk = msg.get("body") or b""
                more = bool(msg.get("more_body"))
                body_accumulator.extend(chunk)
                last_msg_info["size"] = len(body_accumulator)
                if not more:
                    # Attempt to parse JSON-RPC to extract method/id for logging
                    snippet = body_accumulator[:2048]
                    try:
                        import json as _json
                        parsed = _json.loads(snippet.decode("utf-8")) if snippet else None
                        if isinstance(parsed, dict):
                            last_msg_info["id"] = parsed.get("id")
                            last_msg_info["method"] = parsed.get("method")
                        logger.debug(
                            "[SSE] http.request complete: bytes=%d json_keys=%s id=%s method=%s",
                            len(body_accumulator),
                            list(parsed.keys()) if isinstance(parsed, dict) else None,
                            last_msg_info["id"],
                            last_msg_info["method"],
                        )
                    except Exception:
                        # Not JSON or too large; log size and a safe snippet
                        safe_snippet = snippet.decode("utf-8", errors="ignore")
                        logger.debug(
                            "[SSE] http.request complete: bytes=%d non-json body snippet=%r",
                            len(body_accumulator),
                            safe_snippet[:256],
                        )
                    finally:
                        body_accumulator.clear()
                else:
                    logger.debug("[SSE] http.request chunk: +%d bytes (total=%d) more_body=%s", len(chunk), len(body_accumulator), more)
            elif mtype == "http.disconnect":
                logger.debug("[SSE] http.disconnect received. Last message id=%s method=%s size=%d bytes", last_msg_info["id"], last_msg_info["method"], last_msg_info["size"])
        except Exception:
            pass
        return msg

    async def logging_send(message):
        try:
            mtype = message.get("type")
            if mtype == "http.response.start":
                status = message.get("status")
                logger.debug("[SSE] http.response.start status=%s for %s %s?%s", status, method, path, query)
            elif mtype == "http.response.body":
                b = message.get("body") or b""
                more = bool(message.get("more_body"))
                logger.debug("[SSE] http.response.body bytes=%d more_body=%s", len(b), more)
        except Exception:
            pass
        return await send(message)

    try:
        async with sse.connect_sse(new_scope, logging_receive, logging_send) as streams:
            try:
                import sys
                print(f"[SSE] Connection established from {client_ip}:{client_port}, starting MCP run loop", file=sys.stderr)
                logger.debug("[SSE] connection established; starting MCP run loop")
            except Exception:
                pass
            try:
                await mcp.run(streams[0], streams[1], mcp.create_initialization_options())
            except Exception as e:
                # Log to both logger and stderr for Claude Desktop logs
                import sys
                error_msg = f"[SSE] MCP run loop error: {e}"
                logger.error(error_msg, exc_info=True)
                print(error_msg, file=sys.stderr)
                import traceback
                traceback.print_exc(file=sys.stderr)
                raise
    except Exception as e:
        try:
            logger.error("[SSE] error during SSE/MCP handling: %s", e, exc_info=True)
        except Exception:
            pass
        # Let exception propagate? We already returned via ASGI send inside transport; best-effort log only.
    finally:
        try:
            if start_ts is not None:
                dur_ms = int((__import__("time").time() - start_ts) * 1000)
            else:
                dur_ms = -1
            logger.debug("[SSE] /messages connection closed (duration=%sms, client=%s:%s) last_msg_id=%s method=%s", dur_ms, client_ip, client_port, last_msg_info["id"], last_msg_info["method"])
        except Exception:
            pass

# Mount the ASGI app at /messages (primary) and /sse (alias)
# Note: mount() requires trailing slash in the URL in some clients; server normalizes either form.
app.mount("/messages", messages_asgi)
app.mount("/sse", messages_asgi)


def is_stdio_mode() -> bool:
    """Detect if we should run in stdio mode (for Claude Desktop) vs HTTP/SSE mode."""
    # Check if stdin is a pipe/not a TTY (indicates stdio transport)
    return not sys.stdin.isatty()


async def stdio_main():
    """Run the MCP server in stdio mode (for Claude Desktop)."""
    logger.info("Starting Govdata MCP Server in stdio mode...")
    logger.info(f"Calcite JAR: {settings.calcite_jar_path}")
    logger.info(f"Calcite Model: {settings.calcite_model_path}")

    try:
        # Initialize Calcite connection
        initialize_connection(settings.calcite_jar_path, settings.calcite_model_path)
        logger.info("Calcite connection initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Calcite connection: {e}", exc_info=True)
        sys.exit(1)

    try:
        # Run stdio server
        async with stdio_server() as (read_stream, write_stream):
            await mcp.run(
                read_stream,
                write_stream,
                mcp.create_initialization_options()
            )
    finally:
        # Cleanup
        try:
            conn = get_connection()
            conn.close()
            logger.info("Calcite connection closed")
        except Exception as e:
            logger.warning(f"Error closing Calcite connection: {e}")


def main():
    """Run the MCP server (auto-detects stdio vs HTTP/SSE mode)."""
    if is_stdio_mode():
        # Run in stdio mode for Claude Desktop
        import asyncio
        asyncio.run(stdio_main())
    else:
        # Run in HTTP/SSE mode for web clients
        logger.info("Starting Govdata MCP Server in HTTP/SSE mode...")
        uvicorn.run(
            "govdata_mcp.server:app",
            host=settings.server_host,
            port=settings.server_port,
            reload=settings.server_reload,
            log_level=settings.log_level.lower()
        )


if __name__ == "__main__":
    main()
