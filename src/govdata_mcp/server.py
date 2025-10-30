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
from .tools import discovery, query, profile, metadata, vector, analytics
from .logging_config import setup_logging

# Configure logging (will write to logs/ directory and console)
setup_logging()
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

When in doubt, quote all identifiers (including aliases) to avoid syntax errors.

PERFORMANCE & TOKEN OPTIMIZATION:
⚡ Push computation to SQL to minimize token usage! Avoid downloading large datasets.
  ✓ Use JOINs instead of multiple queries
  ✓ Use WHERE to filter server-side (not in context)
  ✓ Use GROUP BY, COUNT, SUM, AVG for aggregation
  ✓ Use CTEs and subqueries for complex logic
  ✓ Use LIMIT to control result size
  ✗ Don't download entire tables to filter/process in context

For detailed optimization patterns, see the sql-best-practices resource.

QUERY TIMEOUT:
- Default timeout: 300 seconds (5 minutes)
- For expensive queries (large JOINs, complex aggregations), increase timeout_seconds
- Maximum timeout: 3600 seconds (1 hour)
- Example: Set timeout_seconds=600 for a 10-minute query""",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query to execute"},
                    "limit": {"type": "integer", "description": "Maximum rows to return", "default": 100},
                    "timeout_seconds": {
                        "type": "integer",
                        "description": "Query timeout in seconds. Default: 300 (5 min), Max: 3600 (1 hour). Increase for expensive queries.",
                        "default": 300,
                        "minimum": 1,
                        "maximum": 3600
                    }
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
        Tool(
            name="detect_outliers",
            description="Detect statistical outliers in query results using machine learning (Isolation Forest) or Z-score methods. Returns anomalous rows for investigation.",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query returning data to analyze"},
                    "id_column": {"type": "string", "description": "Column name to use as identifier (for follow-up queries)"},
                    "method": {
                        "type": "string",
                        "enum": ["isolation_forest", "zscore"],
                        "default": "isolation_forest",
                        "description": "Detection method: isolation_forest (ML-based) or zscore (3-sigma rule)"
                    },
                    "contamination": {
                        "type": "number",
                        "minimum": 0.0,
                        "maximum": 0.5,
                        "default": 0.1,
                        "description": "Expected proportion of outliers (0.1 = 10%)"
                    },
                    "features": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Specific columns to analyze (default: all numeric columns)"
                    },
                    "n_samples": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                        "default": 20,
                        "description": "Max number of outlier examples to return"
                    }
                },
                "required": ["sql"]
            }
        ),
        Tool(
            name="cluster_analysis",
            description="Discover natural groupings in data using K-Means or DBSCAN clustering. Returns cluster statistics and sample members.",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query returning data to analyze"},
                    "method": {
                        "type": "string",
                        "enum": ["kmeans", "dbscan"],
                        "default": "kmeans",
                        "description": "Clustering algorithm: kmeans or dbscan (density-based)"
                    },
                    "n_clusters": {
                        "type": "integer",
                        "minimum": 2,
                        "maximum": 20,
                        "default": 5,
                        "description": "Number of clusters for K-Means"
                    },
                    "eps": {
                        "type": "number",
                        "minimum": 0.01,
                        "default": 0.5,
                        "description": "Distance threshold for DBSCAN"
                    },
                    "min_samples": {
                        "type": "integer",
                        "minimum": 2,
                        "default": 5,
                        "description": "Minimum samples per cluster for DBSCAN"
                    },
                    "features": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Specific columns to analyze (default: all numeric columns)"
                    },
                    "id_column": {"type": "string", "description": "Column name to use as identifier"},
                    "n_samples_per_cluster": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 20,
                        "default": 5,
                        "description": "Number of sample rows per cluster"
                    }
                },
                "required": ["sql"]
            }
        ),
        Tool(
            name="correlation_analysis",
            description="Calculate correlation matrix to find relationships between variables. Identifies strong correlations and multicollinearity.",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query returning data to analyze"},
                    "features": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Specific columns to correlate (default: all numeric columns)"
                    },
                    "method": {
                        "type": "string",
                        "enum": ["pearson", "spearman"],
                        "default": "pearson",
                        "description": "Correlation method: pearson (linear) or spearman (rank-based)"
                    },
                    "threshold": {
                        "type": "number",
                        "minimum": 0.0,
                        "maximum": 1.0,
                        "default": 0.0,
                        "description": "Only return correlations with absolute value above threshold"
                    }
                },
                "required": ["sql"]
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
        elif name == "detect_outliers":
            result = analytics.detect_outliers(**arguments)
        elif name == "cluster_analysis":
            result = analytics.cluster_analysis(**arguments)
        elif name == "correlation_analysis":
            result = analytics.correlation_analysis(**arguments)
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
            ),
            Resource(
                uri="govdata://connection-guide",
                name="Direct Connection Guide",
                description="Advanced: How to connect directly to the data source via JDBC (for generating standalone scripts)",
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

## Query Optimization & Performance

**IMPORTANT: Minimize token usage by computing in the database, not in your context.**

### ❌ Inefficient Pattern (High Token Usage)
```python
# Query 1: Download entire reference table
result1 = query_data("SELECT * FROM sec.companies")  # Returns 10,000 rows → many tokens!

# Process in LLM context
filtered_ciks = [row['cik'] for row in result1 if row['industry'] == 'Technology']

# Query 2: Use filtered list
result2 = query_data(f"SELECT * FROM sec.filings WHERE \"cik\" IN ({','.join(filtered_ciks)})")
```

**Problems:**
- Downloads unnecessary data (wastes tokens)
- Requires multiple round-trips
- Processes data in LLM context (slow, expensive)

### ✅ Efficient Pattern (Low Token Usage)
```sql
-- Single query with JOIN and WHERE (computed server-side)
SELECT f.*, c."company_name", c."industry"
FROM sec.filings f
JOIN sec.companies c ON f."cik" = c."cik"
WHERE c."industry" = 'Technology'
  AND f."year" = 2024
LIMIT 100
```

**Benefits:**
- One query, minimal data transfer
- All filtering/joining done in database
- Returns only needed results

### Best Practices for Performance

#### 1. Use JOINs Instead of Multiple Queries (or Subqueries)
**❌ Don't use multiple queries:**
```sql
-- Query 1: Get company info
SELECT "cik", "company_name" FROM sec.companies WHERE "cik" = '0000320193'
-- Query 2: Get filings
SELECT * FROM sec.filings WHERE "cik" = '0000320193'
```

**❌ Don't use correlated subqueries (known issues):**
```sql
SELECT * FROM sec.companies c
WHERE EXISTS (
    SELECT 1 FROM sec.filings f
    WHERE f."cik" = c."cik" AND f."year" = 2024
)
```

**✅ Do use JOINs (reliable and fast):**
```sql
SELECT c."company_name", f."filing_date", f."form_type"
FROM sec.filings f
JOIN sec.companies c ON f."cik" = c."cik"
WHERE c."cik" = '0000320193'

-- Or for filtering by existence:
SELECT DISTINCT c.*
FROM sec.companies c
JOIN sec.filings f ON f."cik" = c."cik"
WHERE f."year" = 2024
```

#### 2. Filter Early with WHERE Clauses
**❌ Don't:** Download everything and filter in context
```sql
SELECT * FROM econ.fred_series  -- Returns millions of rows!
```

**✅ Do:** Filter server-side
```sql
SELECT "year", "value"
FROM econ.fred_series
WHERE "series_id" = 'UNRATE'
  AND "year" BETWEEN 2020 AND 2024
```

#### 3. Use Aggregates (GROUP BY, COUNT, SUM, AVG)
**❌ Don't:** Download raw data to calculate statistics
```sql
SELECT "value" FROM econ.fred_series WHERE "series_id" = 'UNRATE'
-- Then calculate average in LLM context
```

**✅ Do:** Aggregate in database
```sql
SELECT
    "year",
    AVG("value") as "avg_rate",
    MIN("value") as "min_rate",
    MAX("value") as "max_rate",
    COUNT(*) as "data_points"
FROM econ.fred_series
WHERE "series_id" = 'UNRATE'
GROUP BY "year"
ORDER BY "year"
```

#### 4. Use CTEs for Multi-Step Logic (Avoid Correlated Subqueries)
**✅ Multi-step analysis with CTE:**
```sql
WITH monthly_avg AS (
    SELECT
        "year",
        "month",
        AVG("value") as "avg_value"
    FROM econ.fred_series
    WHERE "series_id" = 'UNRATE'
    GROUP BY "year", "month"
)
SELECT
    "year",
    "month",
    "avg_value",
    "avg_value" - LAG("avg_value") OVER (ORDER BY "year", "month") as "change"
FROM monthly_avg
ORDER BY "year", "month"
```

#### 5. Use LIMIT to Control Result Size
Always include LIMIT unless you need all rows:
```sql
SELECT * FROM large_table LIMIT 100  -- Returns manageable dataset
```

#### 6. Leverage Window Functions for Analytics
**✅ Calculate rankings and running totals in SQL:**
```sql
SELECT
    "company_name",
    "revenue",
    RANK() OVER (ORDER BY "revenue" DESC) as "rank",
    SUM("revenue") OVER (ORDER BY "revenue" DESC) as "running_total"
FROM sec.company_metrics
WHERE "year" = 2024
```

### Cross-Schema Joins
All schemas share the same DuckDB database, enabling powerful cross-schema queries:

```sql
-- Join economic data with SEC filings
SELECT
    f."cik",
    f."filing_date",
    e."value" as "unemployment_rate"
FROM sec.filings f
JOIN econ.fred_series e
    ON EXTRACT(YEAR FROM f."filing_date") = e."year"
    AND EXTRACT(MONTH FROM f."filing_date") = e."month"
WHERE e."series_id" = 'UNRATE'
  AND f."form_type" = '10-K'
LIMIT 100
```

### Performance Summary

**🎯 Golden Rule: Do as much computation in SQL as possible.**

- ✅ JOINs instead of multiple queries
- ✅ WHERE clauses for filtering
- ✅ GROUP BY for aggregation
- ✅ Window functions for analytics
- ✅ CTEs for complex logic
- ✅ LIMIT to control result size
- ❌ Avoid downloading large datasets to process in context

**Result:** Faster queries, lower token usage, better performance.

## Known SQL Limitations & Workarounds

While most SQL features work well, there are some known issues with certain query patterns.

### ⚠️ Correlated Subqueries - Use with Extreme Caution

**Known Issues:**
- Multi-level nested correlated subqueries may return **incorrect results**
- DELETE statements with correlated WHERE clauses may **delete all rows** instead of filtered rows
- Correlated subqueries with aggregate functions can produce **wrong answers**
- Correlated subqueries in HAVING clauses are **not supported**

**Recommendation: Always prefer JOINs over correlated subqueries.**

**❌ Avoid (may produce incorrect results):**
```sql
-- Correlated subquery - has known issues
SELECT * FROM sec.companies c
WHERE EXISTS (
    SELECT 1 FROM sec.filings f
    WHERE f."cik" = c."cik"
      AND f."year" = 2024
)
```

**✅ Use instead (reliable and faster):**
```sql
-- JOIN-based approach - works correctly
SELECT DISTINCT c.*
FROM sec.companies c
JOIN sec.filings f ON f."cik" = c."cik"
WHERE f."year" = 2024
```

**❌ Especially avoid nested correlated subqueries:**
```sql
-- Two-level nested - produces wrong results!
SELECT * FROM sec.companies c
WHERE EXISTS (
    SELECT 1 FROM sec.filings f
    WHERE f."cik" = c."cik"
      AND EXISTS (
        SELECT 1 FROM sec.prices p
        WHERE p."cik" = f."cik"
      )
)
```

**✅ Use multi-way JOINs instead:**
```sql
-- Reliable alternative
SELECT DISTINCT c.*
FROM sec.companies c
JOIN sec.filings f ON f."cik" = c."cik"
JOIN sec.prices p ON p."cik" = f."cik"
```

### ✅ CTEs (WITH Clause) - Generally Safe

Common Table Expressions work well, including recursive CTEs.

**✅ Simple CTE (works great):**
```sql
WITH high_value_companies AS (
    SELECT "cik", "company_name"
    FROM sec.companies
    WHERE "market_cap" > 1000000000
)
SELECT c.*, f."filing_date"
FROM high_value_companies c
JOIN sec.filings f ON f."cik" = c."cik"
WHERE f."year" = 2024
```

**✅ Recursive CTE (supported):**
```sql
WITH RECURSIVE number_series AS (
    SELECT 1 as "n"
    UNION ALL
    SELECT "n" + 1
    FROM number_series
    WHERE "n" < 10
)
SELECT * FROM number_series
```

**⚠️ One Edge Case:** Avoid naming a CTE the same as an existing table name - can cause alias resolution issues.

### ✅ Window Functions - Mostly Safe

Window functions work well for analytics. Most common patterns are reliable:

**✅ Safe patterns:**
```sql
-- Ranking
SELECT "company_name", "revenue",
       RANK() OVER (ORDER BY "revenue" DESC) as "rank"
FROM sec.company_metrics

-- Running totals
SELECT "year", "value",
       SUM("value") OVER (ORDER BY "year") as "cumulative"
FROM econ.fred_series

-- Period-over-period comparison
SELECT "year", "month", "value",
       LAG("value") OVER (ORDER BY "year", "month") as "prev_value"
FROM econ.fred_series
```

**⚠️ Avoid these patterns:**
- Nested window aggregates: `SUM(SUM("col")) OVER()` - throws exception
- Window functions in LATERAL joins - not supported
- Aggregate functions in PARTITION BY clause - causes errors

### ✅ Standard SQL - Fully Reliable

These patterns are well-tested and performant:
- ✅ Regular JOINs (INNER, LEFT, RIGHT, FULL OUTER)
- ✅ Subqueries in FROM clause (derived tables)
- ✅ IN/NOT IN with static lists
- ✅ UNION/UNION ALL
- ✅ GROUP BY with aggregates
- ✅ ORDER BY, LIMIT, OFFSET

### Summary: Safe SQL Patterns

**Preferred (fastest and most reliable):**
1. JOINs for combining data
2. WHERE for filtering
3. GROUP BY for aggregation
4. Window functions for analytics
5. CTEs for readable complex queries

**Avoid (buggy):**
1. Correlated subqueries (especially nested)
2. Complex window function patterns (nested aggregates)

**When in doubt:** Test your query with a LIMIT first to verify results before running on full dataset.

---

**Note:** For advanced users who want to generate standalone scripts, see the [Direct Connection Guide](govdata://connection-guide).
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

    elif uri == "govdata://connection-guide":
        # Return direct connection guide for generating standalone scripts
        guide = """# Direct Connection Guide (Advanced)

**Audience:** Advanced users who need to generate standalone Python programs that connect directly to the data source.

**When to use:** Only when users explicitly request standalone/portable scripts (e.g., "Write me a Python program to query...").

## Architecture Overview

The govdata-mcp-server uses a multi-layer architecture:

```
Python MCP Server
    ↓ (JPype1)
Apache Calcite JDBC Driver (govdata adapter)
    ↓ (JDBC)
DuckDB Query Engine
    ↓
Data Storage (S3/MinIO Parquet files)
```

## Prerequisites

### 1. Java Development Kit (JDK)
- **Version:** JDK 11 or later
- **Download:** https://adoptium.net/

### 2. Python Dependencies
```bash
pip install JPype1==1.5.0
pip install pandas  # optional, for DataFrame support
```

### 3. Required JAR Files

Download and place in a `lib/` directory:

**Primary JAR** (required):
- `calcite-govdata-1.41.0-SNAPSHOT-all.jar` - Calcite with govdata adapter
- Download from your deployment or build from source

**Optional JARs** (recommended for full functionality):
- `slf4j-reload4j-2.0.13.jar` - For Calcite logging
- `duckdb-jdbc-1.1.3.jar` - DuckDB JDBC driver (may be bundled in main JAR)

## Connection Details

### JDBC Connection String
```
jdbc:calcite:model=${MODEL_JSON_PATH};lex=ORACLE;unquotedCasing=TO_LOWER
```

**Parameters:**
- `model` - Path to Calcite model JSON file (see below)
- `lex=ORACLE` - Use Oracle-style identifier quoting (double quotes)
- `unquotedCasing=TO_LOWER` - Convert unquoted identifiers to lowercase

### Model JSON File

Create a `model.json` file defining your data sources. Minimal example:

```json
{
  "version": "1.0",
  "defaultSchema": "econ",
  "schemas": [
    {
      "name": "econ",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
      "operand": {
        "sources": ["FRED", "BLS"],
        "parquetDir": "s3://govdata-parquet",
        "cacheDir": "s3://govdata-production-cache",
        "startYear": 2010,
        "endYear": 2024,
        "executionEngine": "DUCKDB",
        "duckdb": {
          "database": "shared.duckdb"
        }
      }
    }
  ]
}
```

**Key fields:**
- `factory` - Custom schema factory class for govdata adapter
- `parquetDir` - S3/MinIO path to Parquet data files
- `cacheDir` - S3/MinIO path for caching
- `executionEngine` - Set to "DUCKDB" for best performance
- `duckdb.database` - Shared DuckDB database file

### Environment Variables

Your script will need these environment variables:

```bash
# S3/MinIO Configuration (required)
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_ENDPOINT_OVERRIDE="http://localhost:9000"  # MinIO endpoint

# Government API Keys (optional, depends on data sources)
export FRED_API_KEY="your-fred-api-key"
export BLS_API_KEY="your-bls-api-key"
export BEA_API_KEY="your-bea-api-key"
export CENSUS_API_KEY="your-census-api-key"
```

## Python Code Template

```python
#!/usr/bin/env python3
\"\"\"
Standalone script to query govdata via Apache Calcite.

Prerequisites:
  pip install JPype1 pandas

Environment variables required:
  - AWS_ACCESS_KEY_ID
  - AWS_SECRET_ACCESS_KEY
  - AWS_ENDPOINT_OVERRIDE (for MinIO)
  - FRED_API_KEY, BLS_API_KEY, etc. (depending on data sources)
\"\"\"

import jpype
import jpype.dbapi2 as dbapi2
import sys
import os

# Configuration - REPLACE THESE PLACEHOLDERS
CALCITE_JAR_PATH = "${CALCITE_JAR_PATH}"  # e.g., "/path/to/calcite-govdata-1.41.0-SNAPSHOT-all.jar"
MODEL_JSON_PATH = "${MODEL_JSON_PATH}"    # e.g., "/path/to/model.json"

def initialize_jvm(jar_path):
    \"\"\"Initialize JVM with Calcite JAR.\"\"\"
    if not jpype.isJVMStarted():
        print(f"Starting JVM with JAR: {jar_path}")
        jpype.startJVM(
            classpath=jar_path,
            "-Xmx4g",  # Max heap size
            "-Xms1g",  # Initial heap size
            convertStrings=False
        )
        print("JVM started successfully")
    else:
        print("JVM already running")

def connect_to_calcite(model_path):
    \"\"\"Create JDBC connection to Calcite.\"\"\"
    jdbc_url = f"jdbc:calcite:model={model_path};lex=ORACLE;unquotedCasing=TO_LOWER"
    print(f"Connecting to: {jdbc_url}")

    connection = dbapi2.connect(
        jdbc_url,
        driver="org.apache.calcite.jdbc.Driver"
    )
    print("Connected successfully")
    return connection

def execute_query(connection, sql):
    \"\"\"Execute SQL query and return results.\"\"\"
    cursor = connection.cursor()
    try:
        print(f"Executing: {sql}")
        cursor.execute(sql)

        # Get column names
        columns = [desc[0] for desc in cursor.description] if cursor.description else []

        # Fetch all rows
        rows = cursor.fetchall()

        return columns, rows
    finally:
        cursor.close()

def main():
    \"\"\"Main execution.\"\"\"
    # Verify environment variables
    required_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        print(f"Error: Missing required environment variables: {missing}", file=sys.stderr)
        sys.exit(1)

    # Initialize JVM
    initialize_jvm(CALCITE_JAR_PATH)

    # Connect to Calcite
    connection = connect_to_calcite(MODEL_JSON_PATH)

    try:
        # Example query - REMEMBER TO QUOTE RESERVED WORDS!
        sql = \"\"\"
            SELECT "year", "series_id", "value"
            FROM econ.fred_series
            WHERE "series_id" = 'UNRATE'
              AND "year" >= 2020
            ORDER BY "year"
            LIMIT 10
        \"\"\"

        columns, rows = execute_query(connection, sql)

        # Print results
        print(f"\\nResults ({len(rows)} rows):")
        print(" | ".join(columns))
        print("-" * 60)
        for row in rows:
            print(" | ".join(str(v) for v in row))

    finally:
        connection.close()
        print("\\nConnection closed")

if __name__ == "__main__":
    main()
```

## Usage Instructions

1. **Set up environment:**
   ```bash
   # Install Python dependencies
   pip install JPype1 pandas

   # Set environment variables
   export AWS_ACCESS_KEY_ID="your-key"
   export AWS_SECRET_ACCESS_KEY="your-secret"
   export AWS_ENDPOINT_OVERRIDE="http://localhost:9000"
   ```

2. **Customize the script:**
   - Replace `${CALCITE_JAR_PATH}` with actual JAR path
   - Replace `${MODEL_JSON_PATH}` with actual model.json path
   - Modify the SQL query as needed

3. **Run:**
   ```bash
   python your_script.py
   ```

## Important Notes

### SQL Syntax
- **Always use double quotes for identifiers** (see [SQL Best Practices](govdata://sql-best-practices))
- Reserved words like `"year"`, `"date"`, `"value"` must be quoted
- String literals use single quotes: `'2024-01-01'`

### Data Access
- Data is fetched from government APIs and cached as Parquet files
- First queries may be slow as data is downloaded
- Subsequent queries use cached Parquet files for speed

### Security
- Never commit credentials to version control
- Use environment variables or secure credential management
- Consider using IAM roles for AWS/S3 access in production

### Performance
- JVM heap size (`-Xmx`) may need adjustment for large queries
- DuckDB execution engine provides best performance
- Shared DuckDB database enables cross-schema joins

## Alternatives to JDBC

### Option 1: DuckDB CLI (if data is local)
If you have Parquet files locally, query directly with DuckDB:

```bash
duckdb shared.duckdb
```

```sql
SELECT * FROM read_parquet('econ/fred_series/*.parquet') LIMIT 10;
```

### Option 2: Python DuckDB (native)
```python
import duckdb

conn = duckdb.connect('shared.duckdb')
df = conn.execute(\"\"\"
    SELECT * FROM read_parquet('s3://govdata-parquet/econ/fred_series/*.parquet')
    LIMIT 10
\"\"\").df()
```

Note: This bypasses Calcite entirely and accesses Parquet files directly.

## Troubleshooting

### "ClassNotFoundException: org.apache.calcite.jdbc.Driver"
- Verify JAR path is correct
- Ensure JAR contains the Calcite JDBC driver

### "Cannot find model file"
- Use absolute path for model.json
- Verify file exists and is readable

### "Connection refused" or S3 errors
- Check AWS environment variables
- Verify MinIO/S3 endpoint is accessible
- Test with AWS CLI: `aws s3 ls s3://govdata-parquet/`

### Slow queries
- First query downloads data from APIs (slow)
- Subsequent queries use cached Parquet (fast)
- Check logs for download progress

## Getting Help

- Review MCP server logs for configuration examples
- Check `govdata-model.json` in the server deployment
- See Calcite documentation: https://calcite.apache.org/docs/
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
                                "arguments": [
                                    {
                                        "name": arg.name,
                                        "description": arg.description,
                                        "required": arg.required
                                    }
                                    for arg in (prompt.arguments or [])
                                ]
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
                                "uri": str(resource.uri),
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
