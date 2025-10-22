# Govdata MCP Server

Model Context Protocol (MCP) server for Apache Calcite's govdata adapter. Provides semantic access to US Census data, SEC filings, economic indicators, and geographic data via MCP tools.

## Architecture

```
┌──────────────────────────────┐
│  Python MCP Server           │  ← This repo
│  - FastAPI + SSE transport   │
│  - 9 MCP tools               │
│  - API Key + JWT/OIDC auth   │
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

### 1.5. Required JARs (Logging & DuckDB)

This repo already includes the needed JARs under the lib/ directory (SLF4J binding and DuckDB JDBC). You can skip this step if they are present. If you need to (re)download them:

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
AWS_ENDPOINT_OVERRIDE=http://0.0.0.0:9000
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

#### Optional - Execution Engine
By default, you can use DuckDB as the execution engine for query processing. Configure in your `.env`:
```bash
CALCITE_EXECUTION_ENGINE=DUCKDB
```
If using DuckDB, ensure the DuckDB JDBC JAR is present (see Required JARs). You may also control long-running downloads:
```bash
GOVDATA_DOWNLOAD_TIMEOUT_MINUTES=2147483647
```

### 3. Run the Server

```bash
# Development mode (with auto-reload)
python -m govdata_mcp.server

# Or using the installed script
govdata-mcp

# Production mode
uvicorn govdata_mcp.server:app --host 0.0.0.0 --port 8080
```

The server will start on `http://0.0.0.0:8080`

### 4. Test with Health Check

```bash
curl http://0.0.0.0:8080/health
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
curl -H "X-API-Key: dev-key-12345" http://0.0.0.0:8080/messages
```

Configure in `.env`:
```bash
API_KEYS=key1,key2,key3
```

### JWT/OAuth2 (Advanced)

Add Bearer token to requests:
```bash
curl -H "Authorization: Bearer <your-jwt-token>" http://0.0.0.0:8080/messages
```

You have two options:

1) Locally-signed JWT (simple, not provider-backed)
```bash
# .env
JWT_SECRET_KEY=your-secret-key
JWT_ALGORITHM=HS256
```

2) OIDC Provider Tokens (Azure AD, Google, etc.)

Enable OIDC validation to accept tokens issued by an external identity provider. Configure in `.env`:
```bash
# Enable OIDC/OAuth2 token validation
OIDC_ENABLED=true

# Issuer URL:
#  - Azure AD: https://login.microsoftonline.com/<tenant-id>/v2.0
#  - Google:   https://accounts.google.com
OIDC_ISSUER_URL=https://login.microsoftonline.com/<tenant-id>/v2.0

# Audience expected in tokens:
#  - Azure AD: your Application (client) ID or api://<app-id>
#  - Google:   your OAuth client ID
OIDC_AUDIENCE=<your-client-or-audience>

# Optional overrides
# OIDC_JWKS_URL=  # normally discovered automatically from the issuer
# OIDC_CACHE_TTL_SECONDS=3600

# Security: when OIDC is enabled, local HS256 JWT fallback is DISABLED by default
# Set AUTH_ALLOW_LOCAL_JWT_FALLBACK=true only if you intentionally need to accept
# both provider-issued tokens and locally-signed JWTs.
# AUTH_ALLOW_LOCAL_JWT_FALLBACK=false
```

Notes:
- Ensure you use OIDC_ISSUER_URL (not OIDC_ISSUER) and set OIDC_ENABLED=true.
- With OIDC enabled, locally-signed JWTs are rejected by default; you can enable fallback via AUTH_ALLOW_LOCAL_JWT_FALLBACK=true.
- You do not have to delete JWT_* variables; they are ignored unless local fallback is enabled. For stricter security, you can remove them.

Examples:
- Azure AD (single-tenant):
  - OIDC_ISSUER_URL=https://login.microsoftonline.com/<tenant-id>/v2.0
  - OIDC_AUDIENCE=<your-app-client-id>
- Google:
  - OIDC_ISSUER_URL=https://accounts.google.com
  - OIDC_AUDIENCE=<your-oauth-client-id>

Notes:
- Only validation is performed (signature, expiry, issuer, audience). This server does not host a login UI; obtain tokens from your provider (e.g., OAuth Authorization Code flow in your client) and present them in the Authorization header.
- API keys remain supported and can co-exist with OIDC.

FAQ: What is the “client id” (audience) when using a private JWT/OIDC server?
- The server validates the aud claim in the presented token against OIDC_AUDIENCE. In many providers this value is referred to as the Client ID or API Identifier of the resource you are protecting (this MCP server).
- In practice, set OIDC_AUDIENCE to the identifier you configured for this API in your identity provider. Examples:
  - Keycloak (OIDC):
    - OIDC_ISSUER_URL=https://<your-domain>/realms/<realm-name>
    - OIDC_AUDIENCE=<client-id-of-this-api>
    - Notes: Tokens may include multiple audiences. Ensure the client issuing the token includes this API’s client ID in aud (often done by enabling “Include client audience” or adding this API as an audience/scope).
  - Auth0:
    - OIDC_ISSUER_URL=https://<your-tenant>.auth0.com/
    - OIDC_AUDIENCE=https://api.your-company.internal or a UUID-like API Identifier you configured under Applications → APIs.
    - Notes: In Auth0, APIs have an Identifier that becomes the aud claim. Use that value here (not the application’s client_id unless you configured it as the API Identifier).
  - Azure AD (private tenant):
    - OIDC_ISSUER_URL=https://login.microsoftonline.com/<tenant-id>/v2.0
    - OIDC_AUDIENCE=<Application (client) ID> or api://<app-id> depending on how you configured Expose an API.
  - Google Identity Platform / Firebase Auth (OIDC mode):
    - OIDC_ISSUER_URL=https://accounts.google.com (or your federation issuer)
    - OIDC_AUDIENCE=<your OAuth client ID>
  - Custom OIDC with your own JWKS:
    - OIDC_ISSUER_URL=https://auth.your-domain.com
    - OIDC_AUDIENCE=<your-api-audience>
    - Optionally set OIDC_JWKS_URL=https://auth.your-domain.com/.well-known/jwks.json if discovery is not standard.

What if I have a private JWT server that is not OIDC?
- If you cannot expose a standard OIDC discovery document and JWKS, you can either:
  - Use locally-signed JWTs with HS256 by configuring JWT_SECRET_KEY and JWT_ALGORITHM=HS256. In this mode, OIDC_* settings are not required and aud is not enforced by this server.
  - Or implement an OIDC-compatible JWKS endpoint. Then set OIDC_ENABLED=true, OIDC_ISSUER_URL to your issuer, and optionally OIDC_JWKS_URL to your JWKS if discovery is not available.

Rule of thumb:
- Whatever value ends up in the aud claim of the access token your client presents should match OIDC_AUDIENCE in your .env. That value is usually the API/resource identifier you created in your identity provider for this MCP server.

## Security Notes

- Never commit real API keys, JWT secrets, or tokens to version control. Use `.env` locally and keep only sanitized examples in `.env.example`.
- If any secrets were ever committed, rotate them immediately.
- In production, set a strong `API_KEYS` value or use JWT with a strong `JWT_SECRET_KEY`, and restrict network access to trusted clients.
- Prefer running behind HTTPS (reverse proxy) and monitor logs for unauthorized access attempts.

## MCP Client Configuration

### Claude Desktop (Direct connection — no mcp-remote)

Direct connection is recommended. Do not use an external "mcp-remote" bridge; Claude can connect to HTTP/SSE servers directly.

Update `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "calcite-govdata": {
      "command": "true",
      "url": "http://127.0.0.1:8080/messages",
      "headers": { "X-API-Key": "your-api-key-here" }
    }
  }
}
```

Notes:
- The "command": "true" entry is a known workaround to enable remote-only servers in Claude Desktop.
- Use 127.0.0.1 instead of 0.0.0.0 when possible.
- Restart Claude Desktop after editing the config.

Migration from mcp-remote:
- Remove any prior mcp-remote entries.
- Keep only the URL-based config as shown above.
- You do not need the local `mcp_shim.py` for Claude Desktop; it’s provided only for clients that require stdio-based MCP.

### Claude Desktop via mcp-remote (alternative)

If you prefer or need to use the official mcp-remote bridge, this server is compatible. Example Claude Desktop config using npx mcp-remote:

```json
{
  "mcpServers": {
    "my-remote-server": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "http://127.0.0.1:8080/messages",
        "--header",
        "X-API-KEY:prod-key-67890",
        "--verbose"
      ]
    }
  }
}
```

Notes and tips:
- Header name is case-insensitive. Both `X-API-Key` and `X-API-KEY` work; they must match one of the keys in `API_KEYS` from your .env.
- mcp-remote may first POST an `initialize` request to `/messages` without `session_id`. The server supports this compatibility path and returns 200 OK so the client won’t mark the server as failed.
- After initialization, mcp-remote should open an SSE GET to `/messages` and then POST to the announced endpoint `"/messages?session_id=..."`. The server normalizes trailing slashes and handles redirects.

Troubleshooting mcp-remote:
- If you see `307 Temporary Redirect` followed by a `400/409` in logs, ensure the write channel POST includes the `session_id` from the SSE `endpoint` event. The server logs guidance if it receives a POST without `session_id`.
- Run the bridge in verbose mode (as shown) and set `LOG_LEVEL=DEBUG` in `.env` to see detailed `[SSE]` logs on the server.
- Quick curl checks:
  - Base initialize (compat): `curl -s -H "X-API-Key: <key>" -H "Content-Type: application/json" -d '{"jsonrpc":"2.0","id":0,"method":"initialize","params":{}}' http://127.0.0.1:8080/messages | jq .`
  - SSE stream: `curl -N -H "X-API-Key: <key>" http://127.0.0.1:8080/messages`

### Other MCP Clients

The server implements MCP over HTTP with Server-Sent Events (SSE).

Endpoints:
- Primary: `http://0.0.0.0:8080/messages`
- Alias:   `http://0.0.0.0:8080/sse` (same behavior; provided for clarity)

Usage:
- GET to open the SSE read stream.
- POST to the announced endpoint (includes `session_id`) to send data on the write channel.
- Compatibility: a POST initialize to the base path (without `session_id`) returns 200 OK so some clients (e.g., mcp-remote) won’t mark the server as failed.

Transport and Auth:
- **Transport**: SSE (Server-Sent Events)
- **Authentication**: X-API-Key header or Authorization: Bearer token

Direct mode quick test (curl):
- Initialize without session_id (compat path):
  - curl -s -H "X-API-Key: <key>" -H "Content-Type: application/json" \
    -d '{"jsonrpc":"2.0","id":0,"method":"initialize","params":{}}' \
    http://127.0.0.1:8080/messages | jq .
- Open SSE stream (observe endpoint event):
  - curl -N -H "X-API-Key: <key>" http://127.0.0.1:8080/messages

FAQ: Is /messages for Streamable HTTP and /sse for SSE?
- No hard split is required. This server uses a single ASGI handler for both and exposes two paths for convenience:
  - `/messages` is the primary endpoint for MCP over HTTP/SSE.
  - `/sse` is an alias that behaves identically.
- Both support:
  - SSE GET to establish the read stream (you’ll receive an `endpoint` event with `?session_id=...`).
  - POST to the announced endpoint (including `session_id`) for the write channel.
  - A compatibility path where a base-path POST with `{ "method": "initialize" }` gets a 200 OK response so legacy clients don’t fail fast.
- Recommendation: point clients to `/messages` unless you have a policy or tooling preference for `/sse`. Both are equivalent in this server.

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

## Working Without the MCP Server (No Java/Calcite)

If you don’t have access to the Calcite govdata MCP server or don’t want to run Java yet, you can still fetch public employment data directly using the included example script.

What you can do right now
- Query U.S. Census ACS employment profile metrics (DP03) by state
- Query BLS time series (e.g., total nonfarm employment)
- No JVM or Calcite JAR required

Prereqs
- Have Python deps installed: pip install -r requirements.txt (requests is already included)
- Put your API keys in .env (at least CENSUS_API_KEY and/or BLS_API_KEY)
- Export them into your environment when running the script, e.g.:
  - export $(grep -E '^(CENSUS_API_KEY|BLS_API_KEY)=' .env | xargs)

Run examples
- Census (ACS 1-year DP03 profile – employment):
  - python examples/census_employment_example.py census --state CA --year 2022 --limit 10
  - Omitting --state will return all states. Use two-letter state code or FIPS.
- BLS (Current Employment Statistics series):
  - python examples/census_employment_example.py bls --series CES0000000001 --start 2024-01 --end 2024-12

Notes
- The script prints JSON to stdout so you can pipe to jq if desired.
- Rate limits apply; see the Census and BLS API docs for details.
- When you’re ready to use Claude/other MCP clients with richer tools and SQL, follow the setup in Quick Start to run the MCP server, then use the Verification Checklist below.

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
- **Data Sources**: Check govdata adapter documentation# govdata-mcp-server



## MCP Client (Claude) Verification Checklist

Use these prompts in Claude Desktop to confirm it is actually using this server. Keep this server running with `LOG_LEVEL=DEBUG` so you can observe requests.

Prereqs
- Claude Desktop config includes:
  {
    "mcpServers": {
      "calcite-govdata": {
        "command": "true",
        "url": "http://0.0.0.0:8080/messages",
        "headers": { "X-API-Key": "<your-api-key>" }
      }
    }
  }
- Restart Claude Desktop after editing its config.

What to ask Claude (copy/paste)
1) Initialization & tools
- “List the available tools exposed by the calcite-govdata MCP server.”
- “What MCP tools are available from the calcite-govdata server?”
Expected logs here: `/messages` GET/POST, an `initialize` message, and tool discovery.

2) Force a simple tool call
- “Using the calcite-govdata MCP server, call the ‘list_schemas’ tool and show me the result.”
Expected logs: `call_tool name=list_schemas` and a JSON array response.

3) List tables in a schema
- “From the calcite-govdata MCP server, run list_tables with schema=census.”
Expected logs: `call_tool name=list_tables arguments={"schema":"census"}`.

4) Describe a table
- “Use the calcite-govdata MCP tool describe_table for schema=census and table=acs_income.”
Expected logs: `call_tool name=describe_table ...` with column details in response.

5) Minimal query
- “Using the calcite-govdata MCP server, call query_data with sql='SELECT 1 AS one' and limit=1.”
Expected logs: `call_tool name=query_data` with one row `{ "one": 1 }`.

6) Real data smoke test
- “With the calcite-govdata MCP server, call sample_table for schema=census table=population_estimates limit=5.”
Expected logs: `call_tool name=sample_table ...` and a few rows returned.

7) Error-path check
- “Use list_tables with schema=not_a_schema and show the result.”
Expected: server logs an error for that tool call; Claude returns an error payload/explanation.

8) Authentication confirmation
Look for one of these during first connection:
- `Auth: OIDC enabled (issuer=..., audience=..., ...)` OR
- `Auth: OIDC disabled. Accepting API keys and local JWT (...)`
Also on each request: `[SSE] /messages auth succeeded ... mode=API Key|Bearer`.

9) SSE handshake correctness
- After Claude connects: `Sent endpoint event: /messages?session_id=...` and periodic pings.
- If you ever see a `POST /messages` without `session_id`, the server returns 400 with guidance (indicates a misrouted client), but Claude Desktop should post to the session URL automatically.

If Claude doesn’t use the server for a natural-language question
- Ask: “Using the calcite-govdata MCP server, find 5 table names related to employment in the census schema.”
- If no tool call appears in logs, force usage: “You must use the calcite-govdata MCP tools to answer. Start by calling list_schemas.”

Troubleshooting
- Config name must match (`calcite-govdata`) and URL reachable from Claude.
- API key in Claude must match `API_KEYS`.
- Try `http://127.0.0.1:8080/messages` if loopback issues arise.
- Keep `LOG_LEVEL=DEBUG` to see `[SSE]` and `call_tool` lines.
