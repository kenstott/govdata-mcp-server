"""Discovery tools for exploring database schemas, tables, and columns."""

from typing import List, Dict, Any
from ..jdbc import get_connection
import logging

logger = logging.getLogger(__name__)


def list_schemas() -> Dict[str, Any]:
    """
    List all available database schemas in the Calcite data lake.

    Returns:
        Dictionary with list of schema names
    """
    conn = get_connection()
    sql = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME"

    try:
        results = conn.execute_metadata_query(sql)
        schemas = [row["SCHEMA_NAME"] for row in results]
        logger.info(f"Found {len(schemas)} schemas")
        return {"schemas": schemas}
    except Exception as e:
        logger.error(f"Error listing schemas: {e}")
        raise


def list_tables(schema: str, include_comments: bool = False) -> Dict[str, Any]:
    """
    List all tables in a specific schema.

    Args:
        schema: Schema name
        include_comments: Whether to include table comments

    Returns:
        Dictionary with list of tables
    """
    conn = get_connection()

    if include_comments:
        sql = f"""
            SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, REMARKS
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}'
            ORDER BY TABLE_NAME
        """
    else:
        sql = f"""
            SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}'
            ORDER BY TABLE_NAME
        """

    try:
        tables = conn.execute_metadata_query(sql)
        logger.info(f"Found {len(tables)} tables in schema '{schema}'")
        return {"schema": schema, "tables": tables}
    except Exception as e:
        logger.error(f"Error listing tables in schema '{schema}': {e}")
        raise


def describe_table(schema: str, table: str, include_comments: bool = False) -> Dict[str, Any]:
    """
    Get detailed column information for a specific table.

    Args:
        schema: Schema name
        table: Table name
        include_comments: Whether to include column comments and vector metadata

    Returns:
        Dictionary with column details
    """
    conn = get_connection()

    if include_comments:
        sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, REMARKS
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """
    else:
        sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """

    try:
        columns = conn.execute_metadata_query(sql)

        # Check for vector metadata in comments
        if include_comments:
            for col in columns:
                remarks = col.get("REMARKS", "")
                if remarks and "[VECTOR " in remarks:
                    col["has_vector_metadata"] = True

        logger.info(f"Found {len(columns)} columns in table '{schema}.{table}'")
        return {
            "schema": schema,
            "table": table,
            "columns": columns
        }
    except Exception as e:
        logger.error(f"Error describing table '{schema}.{table}': {e}")
        raise