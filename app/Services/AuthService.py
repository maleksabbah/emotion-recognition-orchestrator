"""
AuthService — JWT validation only.

Orchestrator has no users table. The gateway is the issuer of access
tokens; orchestrator only verifies and reads `sub` (user_id) to know
which user is connecting.

Used by the WS route to authenticate the live session — gateway already
authenticates the HTTP routes (internal network, trusted).
"""
from __future__ import annotations

from typing import Any

import jwt

from app.Config import JWT_ALGORITHM, JWT_SECRET
from app.Exceptions import (
    InvalidToken,
    InvalidTokenType,
    MissingAuth,
    TokenExpired,
)


class TokenUser:
    """Lightweight user shape — just id. No DB lookup."""
    def __init__(self, user_id: str):
        self.id = user_id


class AuthService:
    def decode_access_token(self, token: str) -> dict[str, Any]:
        try:
            payload = jwt.decode(
                token, JWT_SECRET, algorithms=[JWT_ALGORITHM]
            )
        except jwt.ExpiredSignatureError:
            raise TokenExpired()
        except jwt.InvalidTokenError:
            raise InvalidToken()

        if payload.get("type") != "access":
            raise InvalidTokenType()
        return payload

    async def resolve_user_from_token(self, token: str) -> TokenUser:
        """Used by WS — JWT comes via ?token=... query param."""
        if not token:
            raise MissingAuth()
        payload = self.decode_access_token(token)
        sub = payload.get("sub")
        if sub is None:
            raise InvalidToken()
        return TokenUser(user_id=str(sub))