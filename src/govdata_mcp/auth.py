"""Authentication middleware for API key and JWT validation."""

from fastapi import Header, HTTPException, Security, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from typing import Optional
from .config import settings
import logging

logger = logging.getLogger(__name__)

# HTTP Bearer scheme for JWT
security = HTTPBearer(auto_error=False)


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
    Verify JWT token from Authorization header.

    Args:
        credentials: JWT credentials from header

    Returns:
        Decoded token payload if valid, None otherwise

    Raises:
        HTTPException: If JWT is invalid
    """
    if not credentials:
        return None

    token = credentials.credentials

    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret_key,
            algorithms=[settings.jwt_algorithm]
        )
        logger.debug(f"Valid JWT authenticated")
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

    Args:
        api_key_valid: Result of API key verification
        jwt_payload: Result of JWT verification

    Returns:
        True if authenticated

    Raises:
        HTTPException: If neither auth method is valid
    """
    if api_key_valid or jwt_payload:
        return True

    raise HTTPException(
        status_code=401,
        detail="Invalid authentication. Provide either X-API-Key header or JWT Bearer token.",
        headers={"WWW-Authenticate": "Bearer"},
    )