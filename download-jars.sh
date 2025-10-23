#!/bin/bash
#
# Download required JARs for govdata-mcp-server
#
# This script downloads the SLF4J binding and DuckDB JDBC driver
# needed for Calcite logging and DuckDB execution engine support.

set -e

LIB_DIR="lib"
SLF4J_VERSION="2.0.13"
DUCKDB_VERSION="1.1.3"

MAVEN_CENTRAL="https://repo1.maven.org/maven2"
SLF4J_URL="$MAVEN_CENTRAL/org/slf4j/slf4j-reload4j/$SLF4J_VERSION/slf4j-reload4j-$SLF4J_VERSION.jar"
DUCKDB_URL="$MAVEN_CENTRAL/org/duckdb/duckdb_jdbc/$DUCKDB_VERSION/duckdb_jdbc-$DUCKDB_VERSION.jar"

SLF4J_JAR="$LIB_DIR/slf4j-reload4j-$SLF4J_VERSION.jar"
DUCKDB_JAR="$LIB_DIR/duckdb-jdbc-$DUCKDB_VERSION.jar"

echo "=== Govdata MCP Server - JAR Dependency Setup ==="
echo ""

# Create lib directory if it doesn't exist
if [ ! -d "$LIB_DIR" ]; then
    echo "Creating $LIB_DIR directory..."
    mkdir -p "$LIB_DIR"
fi

# Download SLF4J binding
if [ -f "$SLF4J_JAR" ]; then
    echo "✓ SLF4J binding already exists: $SLF4J_JAR"
else
    echo "Downloading SLF4J reload4j binding v$SLF4J_VERSION..."
    curl -L -o "$SLF4J_JAR" "$SLF4J_URL"
    echo "✓ Downloaded: $SLF4J_JAR ($(du -h "$SLF4J_JAR" | cut -f1))"
fi

# Download DuckDB JDBC driver
if [ -f "$DUCKDB_JAR" ]; then
    echo "✓ DuckDB JDBC driver already exists: $DUCKDB_JAR"
else
    echo "Downloading DuckDB JDBC driver v$DUCKDB_VERSION..."
    echo "  (This is a large file ~70MB, may take a moment...)"
    curl -L -o "$DUCKDB_JAR" "$DUCKDB_URL"
    echo "✓ Downloaded: $DUCKDB_JAR ($(du -h "$DUCKDB_JAR" | cut -f1))"
fi

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Required JARs are ready in $LIB_DIR/:"
ls -lh "$LIB_DIR"/*.jar 2>/dev/null | awk '{print "  -", $9, "(" $5 ")"}'
echo ""
echo "These JARs will be automatically added to the classpath when the server starts."
echo ""
echo "Next steps:"
echo "  1. Build Calcite JAR from https://github.com/kenstott/calcite"
echo "  2. Configure .env with CALCITE_JAR_PATH and API keys"
echo "  3. Run: python -m govdata_mcp.server"
echo ""
