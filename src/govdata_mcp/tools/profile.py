"""Table profiling tool for statistical analysis."""

from typing import Dict, Any, List, Optional
from ..jdbc import get_connection
import logging

logger = logging.getLogger(__name__)


def profile_table(
    schema: str,
    table: str,
    columns: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Get statistical profile of a table including row count, distinct counts,
    null percentages, and min/max values for each column.

    Args:
        schema: Schema name
        table: Table name
        columns: List of columns to profile (None = all columns)

    Returns:
        Dictionary with profiling statistics
    """
    conn = get_connection()
    qualified_table = f"{schema}.{table}"

    # If no columns specified, get all columns
    if columns is None or len(columns) == 0:
        col_sql = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """
        col_results = conn.execute_metadata_query(col_sql)
        columns = [row["COLUMN_NAME"] for row in col_results]

    # Build profile SQL
    sql_parts = ["SELECT COUNT(*) as row_count"]

    for col in columns:
        # Distinct count
        sql_parts.append(f"COUNT(DISTINCT {col}) as {col}_distinct")
        # Null percentage
        sql_parts.append(
            f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as {col}_null_pct"
        )
        # Min/Max (cast to VARCHAR for compatibility)
        sql_parts.append(f"CAST(MIN({col}) AS VARCHAR) as {col}_min")
        sql_parts.append(f"CAST(MAX({col}) AS VARCHAR) as {col}_max")

    profile_sql = f"{', '.join(sql_parts)} FROM {qualified_table}"

    try:
        cursor = conn.get_cursor()
        cursor.execute(profile_sql)
        row = cursor.fetchone()
        cursor.close()

        if not row:
            raise ValueError(f"No data returned for table {qualified_table}")

        # Parse results
        row_count = row[0]
        column_profiles = []

        idx = 1
        for col in columns:
            col_profile = {
                "name": col,
                "distinct_count": row[idx],
                "null_percentage": row[idx + 1],
                "min": row[idx + 2],
                "max": row[idx + 3]
            }
            column_profiles.append(col_profile)
            idx += 4

        logger.info(f"Profiled table '{qualified_table}' with {len(columns)} columns")
        return {
            "schema": schema,
            "table": table,
            "row_count": row_count,
            "columns": column_profiles
        }
    except Exception as e:
        logger.error(f"Error profiling table '{qualified_table}': {e}")
        raise