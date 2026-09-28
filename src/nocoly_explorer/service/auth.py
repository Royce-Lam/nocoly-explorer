"""Bearer-token API key authentication for the FastAPI service."""

from __future__ import annotations

from typing import Callable, Optional

from fastapi import Header, HTTPException, status


def _check_api_key(authorization: Optional[str], expected_key: str) -> None:
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )
    parts = authorization.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization scheme must be Bearer",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if parts[1] != expected_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )


def require_api_key(api_key: Optional[str]) -> Callable:
    """Build a FastAPI dependency that enforces the API key.

    If `api_key` is None (development mode), the dependency is a no-op.
    """
    if not api_key:
        async def _noop() -> None:
            return None
        return _noop

    async def _dep(authorization: Optional[str] = Header(default=None)) -> None:
        _check_api_key(authorization, api_key)

    return _dep