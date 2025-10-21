"""Metadata search tool for semantic discovery across all database metadata."""

from typing import Dict, Any
from ..jdbc import get_connection
import logging

logger = logging.getLogger(__name__)


def search_metadata(query: str) -> Dict[str, Any]:
    """
    Get ALL database metadata (schemas, tables, columns, comments) for semantic search.

    The LLM performs the actual semantic matching against the user's query.
    This tool returns the complete catalog to enable that matching.

    Args:
        query: User's search query (informational - LLM uses this to match results)

    Returns:
        Dictionary with complete metadata catalog
    """
    conn = get_connection()

    # Get all schemas
    schemas_sql = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME"
    schema_rows = conn.execute_metadata_query(schemas_sql)

    result = {
        "query": query,
        "schemas": []
    }

    # For each schema, get all tables and columns
    for schema_row in schema_rows:
        schema_name = schema_row["SCHEMA_NAME"]
        schema_data = {
            "name": schema_name,
            "tables": []
        }

        # Get tables in this schema
        tables_sql = f"""
            SELECT TABLE_NAME, TABLE_TYPE, REMARKS
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema_name}'
            ORDER BY TABLE_NAME
        """
        table_rows = conn.execute_metadata_query(tables_sql)

        for table_row in table_rows:
            table_name = table_row["TABLE_NAME"]
            table_data = {
                "name": table_name,
                "type": table_row["TABLE_TYPE"],
                "comment": table_row.get("REMARKS", ""),
                "columns": []
            }

            # Get columns for this table
            columns_sql = f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, REMARKS
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION
            """
            column_rows = conn.execute_metadata_query(columns_sql)

            for col_row in column_rows:
                col_data = {
                    "name": col_row["COLUMN_NAME"],
                    "type": col_row["DATA_TYPE"],
                    "nullable": col_row["IS_NULLABLE"] == "YES",
                    "comment": col_row.get("REMARKS", "")
                }

                # Check for vector metadata
                if col_data["comment"] and "[VECTOR " in col_data["comment"]:
                    col_data["has_vector_metadata"] = True

                table_data["columns"].append(col_data)

            schema_data["tables"].append(table_data)

        result["schemas"].append(schema_data)

    logger.info(f"Returned metadata for {len(result['schemas'])} schemas")
    return result