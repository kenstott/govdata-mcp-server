"""
Govdata MCP Server - Model Context Protocol server for Apache Calcite.

Provides MCP tools for querying census data, SEC filings, economic indicators,
and geographic data via Apache Calcite's govdata adapter.
"""

from dotenv import load_dotenv

# Load environment variables from .env file
# This ensures all env vars (including those needed by Calcite JAR) are available
load_dotenv()

__version__ = "0.1.0"