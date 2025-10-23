#!/bin/bash
#
# Start the Govdata MCP Server with prerequisite checks
#
# Usage:
#   ./start-server.sh           # Development mode (auto-reload)
#   ./start-server.sh prod      # Production mode
#   ./start-server.sh --help    # Show help

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
MODE="${1:-dev}"
ENV_FILE=".env"
LIB_DIR="lib"

print_error() {
    echo -e "${RED}✗ Error: $1${NC}" >&2
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ Warning: $1${NC}"
}

print_help() {
    cat << EOF
Govdata MCP Server Startup Script

Usage:
  ./start-server.sh [mode]

Modes:
  dev       Development mode with auto-reload (default)
  prod      Production mode
  --help    Show this help message

Prerequisites:
  - Python 3.9+ with venv activated
  - Java 17+ installed
  - Calcite JAR built from github.com/kenstott/calcite
  - .env file configured with API keys and paths
  - Required JARs in lib/ (run ./download-jars.sh)

Environment Variables (.env):
  CALCITE_JAR_PATH      Path to Calcite govdata JAR
  CALCITE_MODEL_PATH    Path to Calcite model JSON
  API_KEYS              Comma-separated API keys
  FRED_API_KEY          Federal Reserve API key
  BLS_API_KEY           Bureau of Labor Statistics key
  BEA_API_KEY           Bureau of Economic Analysis key
  CENSUS_API_KEY        US Census Bureau key

Examples:
  ./start-server.sh              # Start in dev mode
  ./start-server.sh prod         # Start in production mode
  LOG_LEVEL=DEBUG ./start-server.sh  # Start with debug logging

EOF
}

# Handle --help
if [ "$MODE" = "--help" ] || [ "$MODE" = "-h" ]; then
    print_help
    exit 0
fi

echo "=== Govdata MCP Server Startup Checks ==="
echo ""

# Check 1: Python version
echo "Checking Python..."
if ! command -v python &> /dev/null; then
    print_error "Python not found. Please install Python 3.9+"
    exit 1
fi

PYTHON_VERSION=$(python --version 2>&1 | awk '{print $2}')
print_success "Python $PYTHON_VERSION found"

# Check 2: Java installation
echo "Checking Java..."
if ! command -v java &> /dev/null; then
    print_error "Java not found. Please install Java 17+"
    echo "  Install: brew install openjdk@17 (macOS) or apt install openjdk-17-jdk (Ubuntu)"
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | head -n 1 | awk -F '"' '{print $2}')
print_success "Java $JAVA_VERSION found"

# Check 3: .env file
echo "Checking .env configuration..."
if [ ! -f "$ENV_FILE" ]; then
    print_error ".env file not found"
    echo "  Run: cp .env.example .env"
    echo "  Then edit .env with your configuration"
    exit 1
fi
print_success ".env file exists"

# Check 4: Required environment variables
source "$ENV_FILE"

if [ -z "$CALCITE_JAR_PATH" ]; then
    print_error "CALCITE_JAR_PATH not set in .env"
    echo "  Set path to: govdata/build/libs/calcite-govdata-1.41.0-SNAPSHOT-all.jar"
    exit 1
fi

if [ ! -f "$CALCITE_JAR_PATH" ]; then
    print_error "Calcite JAR not found: $CALCITE_JAR_PATH"
    echo "  Build it from: https://github.com/kenstott/calcite"
    echo "  Run: ./gradlew :govdata:shadowJar"
    exit 1
fi
print_success "Calcite JAR found: $CALCITE_JAR_PATH"

if [ -z "$CALCITE_MODEL_PATH" ]; then
    print_error "CALCITE_MODEL_PATH not set in .env"
    exit 1
fi

if [ ! -f "$CALCITE_MODEL_PATH" ]; then
    print_error "Calcite model not found: $CALCITE_MODEL_PATH"
    exit 1
fi
print_success "Calcite model found: $CALCITE_MODEL_PATH"

if [ -z "$API_KEYS" ]; then
    print_warning "API_KEYS not set in .env - authentication will fail"
fi

# Check 5: Required JARs
echo "Checking required JARs..."
if [ ! -d "$LIB_DIR" ]; then
    print_error "lib/ directory not found"
    echo "  Run: ./download-jars.sh"
    exit 1
fi

if [ ! -f "$LIB_DIR/slf4j-reload4j-2.0.13.jar" ]; then
    print_warning "SLF4J binding not found in lib/"
    echo "  Run: ./download-jars.sh"
fi

if [ ! -f "$LIB_DIR/duckdb-jdbc-1.1.3.jar" ]; then
    print_warning "DuckDB JDBC driver not found in lib/"
    echo "  Run: ./download-jars.sh"
fi

print_success "Required JARs present"

# Check 6: Python package installed
echo "Checking Python package installation..."
if ! python -c "import govdata_mcp" 2>/dev/null; then
    print_error "govdata_mcp package not installed"
    echo "  Run: pip install -e ."
    exit 1
fi
print_success "govdata_mcp package installed"

# Check 7: Government API keys
echo "Checking API keys..."
MISSING_KEYS=0
for KEY_VAR in FRED_API_KEY BLS_API_KEY BEA_API_KEY CENSUS_API_KEY; do
    if [ -z "${!KEY_VAR}" ]; then
        print_warning "$KEY_VAR not set in .env"
        MISSING_KEYS=$((MISSING_KEYS + 1))
    fi
done

if [ $MISSING_KEYS -eq 0 ]; then
    print_success "All government API keys configured"
else
    echo "  Some data sources may not work without API keys"
fi

echo ""
echo "=== Starting Server ==="
echo ""

# Set default log level if not set
if [ -z "$LOG_LEVEL" ]; then
    export LOG_LEVEL="INFO"
fi

# Start server based on mode
if [ "$MODE" = "prod" ] || [ "$MODE" = "production" ]; then
    echo "Starting in PRODUCTION mode..."
    echo "  Host: ${SERVER_HOST:-0.0.0.0}"
    echo "  Port: ${SERVER_PORT:-8080}"
    echo "  Log Level: $LOG_LEVEL"
    echo ""

    exec uvicorn govdata_mcp.server:app \
        --host "${SERVER_HOST:-0.0.0.0}" \
        --port "${SERVER_PORT:-8080}" \
        --log-level "$(echo $LOG_LEVEL | tr '[:upper:]' '[:lower:]')" \
        --no-access-log

elif [ "$MODE" = "dev" ] || [ "$MODE" = "development" ]; then
    echo "Starting in DEVELOPMENT mode (with auto-reload)..."
    echo "  Host: ${SERVER_HOST:-0.0.0.0}"
    echo "  Port: ${SERVER_PORT:-8080}"
    echo "  Log Level: $LOG_LEVEL"
    echo ""

    exec uvicorn govdata_mcp.server:app \
        --host "${SERVER_HOST:-0.0.0.0}" \
        --port "${SERVER_PORT:-8080}" \
        --reload \
        --log-level "$(echo $LOG_LEVEL | tr '[:upper:]' '[:lower:]')"

else
    print_error "Invalid mode: $MODE"
    echo "  Use: dev, prod, or --help"
    exit 1
fi
