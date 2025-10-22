"""Configuration management for Govdata MCP Server."""

from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    """Application settings loaded from environment variables or .env file."""

    # Calcite JDBC Configuration
    calcite_jar_path: str
    calcite_model_path: str

    # MCP Server Configuration
    server_host: str = "0.0.0.0"
    server_port: int = 8080
    server_reload: bool = False

    # Authentication
    api_keys: str = "dev-key-12345"  # Comma-separated
    jwt_secret_key: str = "change-this-in-production"
    jwt_algorithm: str = "HS256"
    jwt_access_token_expire_minutes: int = 30

    # OIDC / OAuth2 provider validation
    oidc_enabled: bool = False
    oidc_issuer_url: str | None = None  # e.g., https://login.microsoftonline.com/<tenant>/v2.0 or https://accounts.google.com
    oidc_audience: str | None = None    # your API/application client ID or audience expected in tokens
    oidc_jwks_url: str | None = None    # optional override; if not set, discovered from issuer
    oidc_cache_ttl_seconds: int = 3600

    # Security toggles
    auth_allow_local_jwt_fallback: bool = False  # When OIDC is enabled, allow HS256 fallback only if explicitly set

    # Logging
    log_level: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"  # Allow extra env vars for Calcite JAR

    @property
    def api_keys_list(self) -> List[str]:
        """Return API keys as a list."""
        return [key.strip() for key in self.api_keys.split(",") if key.strip()]


# Global settings instance
settings = Settings()
