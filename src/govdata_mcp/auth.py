"""Authentication middleware for API key and JWT/OIDC validation."""

from fastapi import Header, HTTPException, Security, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from typing import Optional, Dict, Any
from .config import settings
import logging
import time
import json
from urllib.request import urlopen, Request

logger = logging.getLogger(__name__)

# HTTP Bearer scheme for JWT
security = HTTPBearer(auto_error=False)

# Simple in-memory JWKS cache
_jwks_cache: Dict[str, Any] = {"keys": None, "expires_at": 0, "jwks_url": None}
_openid_config_cache: Dict[str, Any] = {"config": None, "expires_at": 0, "issuer": None}


def _http_get_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "govdata-mcp-server"})
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _discover_openid_configuration(issuer: str) -> dict:
    now = time.time()
    if (
        _openid_config_cache["config"]
        and _openid_config_cache["issuer"] == issuer
        and _openid_config_cache["expires_at"] > now
    ):
        return _openid_config_cache["config"]

    url = issuer.rstrip("/") + "/.well-known/openid-configuration"
    config = _http_get_json(url)
    # cache for 1 hour
    _openid_config_cache.update(
        {"config": config, "expires_at": now + 3600, "issuer": issuer}
    )
    return config


def _load_jwks(issuer: str) -> dict:
    now = time.time()
    jwks_url = settings.oidc_jwks_url
    if not jwks_url:
        openid = _discover_openid_configuration(issuer)
        jwks_url = openid["jwks_uri"]

    if (
        _jwks_cache["keys"]
        and _jwks_cache["jwks_url"] == jwks_url
        and _jwks_cache["expires_at"] > now
    ):
        return _jwks_cache["keys"]

    jwks = _http_get_json(jwks_url)
    # cache for configured TTL
    ttl = max(60, int(settings.oidc_cache_ttl_seconds))
    _jwks_cache.update(
        {
            "keys": jwks,
            "expires_at": now + ttl,
            "jwks_url": jwks_url,
        }
    )
    try:
        logger.info("Auth: OIDC JWKS fetched from %s (cache TTL=%ss)", jwks_url, ttl)
    except Exception:
        pass
    return jwks


def verify_oidc_token(token: str) -> Optional[dict]:
    """Validate a JWT against the configured OIDC provider using JWKS."""
    if not settings.oidc_enabled or not settings.oidc_issuer_url:
        return None

    try:
        unverified_header = jwt.get_unverified_header(token)
        kid = unverified_header.get("kid")
    except JWTError as e:
        logger.warning(f"Invalid token header: {e}")
        return None

    try:
        jwks = _load_jwks(settings.oidc_issuer_url)
        public_key = None
        for key in jwks.get("keys", []):
            if key.get("kid") == kid:
                public_key = key
                break
        if public_key is None:
            # cache bust and retry once
            _jwks_cache.update({"keys": None, "expires_at": 0})
            jwks = _load_jwks(settings.oidc_issuer_url)
            for key in jwks.get("keys", []):
                if key.get("kid") == kid:
                    public_key = key
                    break
        if public_key is None:
            logger.warning("OIDC key with matching kid not found in JWKS")
            return None

        payload = jwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            audience=settings.oidc_audience,
            issuer=settings.oidc_issuer_url,
            options={"verify_at_hash": False},
        )
        logger.debug("Valid OIDC JWT authenticated")
        return payload
    except JWTError as e:
        logger.warning(f"Invalid OIDC JWT: {e}")
        return None
    except Exception as e:
        logger.error(f"Error validating OIDC token: {e}")
        return None


async def verify_api_key(x_api_key: Optional[str] = Header(None)) -> bool:
    """
    Verify API key from X-API-Key header.

    Args:
        x_api_key: API key from header

    Returns:
        True if valid

    Raises:
        HTTPException: If API key is invalid
    """
    if x_api_key and x_api_key in settings.api_keys_list:
        logger.debug(f"Valid API key authenticated")
        return True
    return False


async def verify_jwt(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security)
) -> Optional[dict]:
    """
    Verify JWT token from Authorization header. Uses OIDC if enabled, otherwise symmetric JWT.
    """
    if not credentials:
        return None

    token = credentials.credentials

    # Try OIDC first if enabled
    if settings.oidc_enabled:
        payload = verify_oidc_token(token)
        if payload is not None:
            return payload
        # Only allow local HS256 fallback if explicitly enabled
        if not settings.auth_allow_local_jwt_fallback:
            return None

    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret_key,
            algorithms=[settings.jwt_algorithm],
        )
        logger.debug(f"Valid local JWT authenticated")
        return payload
    except JWTError as e:
        logger.warning(f"Invalid JWT: {e}")
        return None


async def verify_auth(
    api_key_valid: bool = Depends(verify_api_key),
    jwt_payload: Optional[dict] = Depends(verify_jwt)
) -> bool:
    """
    Verify either API key or JWT authentication.

    At least one must be valid.
    """
    if api_key_valid or jwt_payload:
        return True

    raise HTTPException(
        status_code=401,
        detail="Invalid authentication. Provide either X-API-Key header or JWT Bearer token.",
        headers={"WWW-Authenticate": "Bearer"},
    )


def headers_authenticated(headers: Dict[str, str]) -> bool:
    """Authenticate using raw headers (for ASGI endpoints). Supports API key and JWT/OIDC."""
    # API Key
    api_key = headers.get("x-api-key")
    if api_key and api_key in settings.api_keys_list:
        return True

    # Bearer
    auth_header = headers.get("authorization")
    if auth_header and auth_header.lower().startswith("bearer "):
        token = auth_header.split(" ", 1)[1]
        # OIDC first if enabled
        if settings.oidc_enabled:
            payload = verify_oidc_token(token)
            if payload is not None:
                return True
            # Only fallback to local JWT if explicitly allowed
            if not settings.auth_allow_local_jwt_fallback:
                return False
        # fallback to local JWT secret
        try:
            jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
            return True
        except JWTError:
            return False

    return False
