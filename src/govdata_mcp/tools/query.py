"""Query tools for executing SQL and sampling tables."""

from typing import Dict, Any
from ..jdbc import get_connection
import logging

logger = logging.getLogger(__name__)


def query_data(sql: str, limit: int = 100) -> Dict[str, Any]:
    """
    Execute a SQL query against the Calcite data lake and return results.

    Args:
        sql: SQL query to execute
        limit: Maximum number of rows to return (default 100)

    Returns:
        Dictionary with columns and rows
    """
    conn = get_connection()

    # Apply limit if not already present
    query = sql.strip()
    if limit > 0 and "LIMIT" not in query.upper():
        query = f"{query} LIMIT {limit}"

    try:
        columns, rows = conn.execute_query(query)
        # Convert rows to list of lists for JSON serialization
        rows_list = [list(row) for row in rows]

        logger.info(f"Query returned {len(rows_list)} rows with {len(columns)} columns")
        return {
            "columns": columns,
            "rows": rows_list,
            "row_count": len(rows_list)
        }
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise


def sample_table(schema: str, table: str, limit: int = 10) -> Dict[str, Any]:
    """
    Get a sample of rows from a specific table.

    Args:
        schema: Schema name
        table: Table name
        limit: Number of rows to sample (default 10)

    Returns:
        Dictionary with columns and rows
    """
    qualified_table = f"{schema}.{table}"
    sql = f"SELECT * FROM {qualified_table}"

    try:
        result = query_data(sql, limit)
        result["schema"] = schema
        result["table"] = table
        logger.info(f"Sampled {result['row_count']} rows from {qualified_table}")
        return result
    except Exception as e:
        logger.error(f"Error sampling table '{qualified_table}': {e}")
        raise