# Govdata MCP Server

Model Context Protocol (MCP) server for Apache Calcite's govdata adapter. Provides semantic access to US Census data, SEC filings, economic indicators, and geographic data via MCP tools.

## Architecture

```
┌──────────────────────────────┐
│  Python MCP Server           │  ← This repo
│  - FastAPI + SSE transport   │
│  - 9 MCP tools               │
│  - API Key + JWT auth        │
└──────────┬───────────────────┘
           │ JPype1 (JVM bridge)
           ▼
┌──────────────────────────────┐
│  Calcite Fat JAR             │  ← Built from calcite repo
│  - JDBC driver               │
│  - Govdata adapter           │
│  - DuckDB sub-schema         │
└──────────────────────────────┘
```

## Prerequisites

- **Python 3.9+**
- **Java 17+** (required by Calcite JAR)
- **Calcite Fat JAR** - Build from calcite repo:
  ```bash
  cd /path/to/calcite
  ./gradlew :govdata:shadowJar
  # JAR will be at: govdata/build/libs/calcite-govdata-1.41.0-SNAPSHOT-all.jar
  ```

## Quick Start

### 1. Install Dependencies

```bash
cd govdata-mcp-server
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
pip install -e .  # Install the package in editable mode
```

### 1.5. Download Required JARs (Logging & DuckDB)

The Calcite JAR requires additional dependencies that aren't included in the fat JAR:

```bash
mkdir -p lib

# SLF4J 2.x binding for logging
curl -L -o lib/slf4j-reload4j-2.0.13.jar https://repo1.maven.org/maven2/org/slf4j/slf4j-reload4j/2.0.13/slf4j-reload4j-2.0.13.jar

# DuckDB JDBC driver (required if using DuckDB execution engine)
curl -L -o lib/duckdb-jdbc-1.1.3.jar https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/1.1.3/duckdb_jdbc-1.1.3.jar
```

These JARs will be automatically added to the classpath before the Calcite JAR.

### 2. Configure Environment

Copy `.env.example` to `.env` and update paths:

```bash
cp .env.example .env
```

Edit `.env` and configure the following:

**Required - Calcite Configuration:**
```bash
CALCITE_JAR_PATH=/path/to/calcite/govdata/build/libs/calcite-govdata-1.41.0-SNAPSHOT-all.jar
CALCITE_MODEL_PATH=/path/to/calcite/djia-production-model.json
```

**Required - MCP Server Authentication:**
```bash
API_KEYS=your-api-key-here
```

**Required - AWS/S3 Configuration (for MinIO or AWS S3):**
```bash
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_ENDPOINT_OVERRIDE=http://localhost:9000
GOVDATA_PARQUET_DIR=s3://govdata-parquet
GOVDATA_CACHE_DIR=s3://govdata-production-cache
```

**Required - Government Data API Keys:**

The Calcite govdata adapter requires API keys for various government data sources. Register for free at:

- **FRED API** (https://fred.stlouisfed.org/docs/api/api_key.html)
- **BLS API** (https://www.bls.gov/developers/api_signature_v2.html)
- **BEA API** (https://apps.bea.gov/API/signup/)
- **Census API** (https://api.census.gov/data/key_signup.html)

Add these to `.env`:
```bash
FRED_API_KEY=your-fred-api-key
BLS_API_KEY=your-bls-api-key
BEA_API_KEY=your-bea-api-key
CENSUS_API_KEY=your-census-api-key
```

See `.env.example` for additional optional API keys (FBI, NHTSA, FEMA, HUD, etc.).

### 3. Run the Server

```bash
# Development mode (with auto-reload)
python -m govdata_mcp.server

# Or using the installed script
govdata-mcp

# Production mode
uvicorn govdata_mcp.server:app --host 0.0.0.0 --port 8080
```

The server will start on `http://localhost:8080`

### 4. Test with Health Check

```bash
curl http://localhost:8080/health
```

## Available MCP Tools

The server exposes 9 MCP tools:

### Discovery Tools
- **list_schemas** - List all database schemas
- **list_tables** - List tables in a schema
- **describe_table** - Get column details for a table

### Query Tools
- **query_data** - Execute SQL queries
- **sample_table** - Sample rows from a table

### Analysis Tools
- **profile_table** - Statistical profiling (row count, distinct counts, min/max, nulls)
- **search_metadata** - Semantic search across all metadata

### Vector Search Tools
- **semantic_search** - Vector similarity search on embedded data
- **list_vector_sources** - List source tables for multi-source vectors

## Authentication

The server supports two authentication methods:

### API Key (Simple)

Add header to requests:
```bash
curl -H "X-API-Key: dev-key-12345" http://localhost:8080/messages
```

Configure in `.env`:
```bash
API_KEYS=key1,key2,key3
```

### JWT/OAuth2 (Advanced)

Add Bearer token to requests:
```bash
curl -H "Authorization: Bearer <your-jwt-token>" http://localhost:8080/messages
```

Configure in `.env`:
```bash
JWT_SECRET_KEY=your-secret-key
JWT_ALGORITHM=HS256
```

## MCP Client Configuration

### Claude Desktop

Update `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "calcite-govdata": {
      "url": "http://localhost:8080/messages",
      "headers": {
        "X-API-Key": "your-api-key-here"
      }
    }
  }
}
```

### Other MCP Clients

The server implements MCP over HTTP with Server-Sent Events (SSE). Connect to:
- **Endpoint**: `http://localhost:8080/messages`
- **Method**: GET or POST
- **Transport**: SSE (Server-Sent Events)
- **Authentication**: X-API-Key header or Authorization: Bearer token

## Example Queries

### List Available Schemas

```python
# Via MCP tool call
{
  "tool": "list_schemas",
  "arguments": {}
}
```

### Query Census Data

```python
{
  "tool": "query_data",
  "arguments": {
    "sql": "SELECT state_fips, population_estimate FROM census.population_estimates WHERE year = 2020 LIMIT 10",
    "limit": 100
  }
}
```

### Profile a Table

```python
{
  "tool": "profile_table",
  "arguments": {
    "schema": "census",
    "table": "acs_income",
    "columns": ["median_household_income", "poverty_rate"]
  }
}
```

## Development

### Project Structure

```
govdata-mcp-server/
├── src/govdata_mcp/
│   ├── __init__.py
│   ├── server.py          # Main MCP server
│   ├── config.py          # Configuration management
│   ├── jdbc.py            # JDBC connection via JPype
│   ├── auth.py            # Authentication middleware
│   └── tools/
│       ├── discovery.py   # Schema/table discovery
│       ├── query.py       # SQL execution
│       ├── profile.py     # Table profiling
│       ├── metadata.py    # Metadata search
│       └── vector.py      # Vector similarity search
├── tests/
├── .env                   # Environment configuration
├── .env.example           # Environment template
├── log4j.properties       # JVM logging configuration
├── pyproject.toml
├── requirements.txt
└── README.md
```

### Running Tests

```bash
pytest tests/
```

### Code Formatting

```bash
black src/
ruff check src/
mypy src/
```

## Docker Deployment

### Build Image

```bash
docker build -t govdata-mcp-server .
```

### Run with Docker Compose

```bash
docker-compose up
```

## Logging Configuration

The server uses log4j for JVM-side logging (Calcite, AWS SDK) and Python's standard logging for the MCP server.

### JVM Logging (log4j.properties)

Configure Java logging in `log4j.properties`:

```properties
# Root logger
log4j.rootLogger=INFO, stdout

# Reduce AWS SDK verbosity
log4j.logger.com.amazonaws=WARN

# Calcite logging
log4j.logger.org.apache.calcite=INFO

# Govdata adapter - DEBUG shows detailed operations (data loading, queries, etc.)
log4j.logger.org.apache.calcite.adapter.govdata=DEBUG
```

**Note**: The govdata adapter logging level is also controlled by the JVM system property `-Dorg.apache.calcite.adapter.govdata.level=DEBUG` which is set in `jdbc.py`. Both must be configured for detailed logging.

### Python Logging

Set log level in `.env`:
```bash
LOG_LEVEL=INFO  # Options: DEBUG, INFO, WARN, ERROR
```

### Startup Warnings

You may see SLF4J warnings during startup:
```
SLF4J(W): No SLF4J providers were found.
SLF4J(W): Defaulting to no-operation (NOP) logger implementation
```

These warnings are harmless and can be safely ignored. They appear because the Calcite JAR contains SLF4J bindings but no provider. Logging is handled by log4j instead.

## Troubleshooting

### JVM Not Starting

- Ensure Java 17+ is installed: `java -version`
- Check Calcite JAR path is correct
- Verify JAR file exists and is readable
- Check JVM memory settings in `jdbc.py` (default: 8GB max, 2GB initial)

### Connection Errors

- Check Calcite model JSON path
- Ensure MinIO is running (if using S3 backend)
- Verify environment variables in `.env`
- Enable debug logging: Set `log4j.logger.org.apache.calcite.adapter.govdata=DEBUG` in `log4j.properties`

### Authentication Failures

- Check API key matches `.env` configuration
- For JWT, verify secret key and algorithm
- Ensure header name is correct (`X-API-Key` or `Authorization`)

## Performance Notes

- **JVM Startup**: ~1-2 seconds on first connection
- **Query Speed**: Native JDBC performance after warmup
- **Memory**: Python process + JVM (allocate ~2GB for Java)

## License

Apache License 2.0

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Support

For issues related to:
- **MCP Server**: Open issue in this repo
- **Calcite/JDBC**: Open issue in calcite repo
- **Data Sources**: Check govdata adapter documentation