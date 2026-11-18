"""
AuthService — JWT validation only.

Orchestrator has no users table. The gateway is the issuer of access
tokens; orchestrator only verifies and reads `sub` (user_id) to know
which user is connecting, plus `plan` / `is_admin` for live-mode limits.

Used by the WS route to authenticate the live session — gateway already
authenticates the HTTP routes (internal network, trusted).
"""
from __future__ import annotations

from typing import Any

import jwt

from app.Config import DEFAULT_PLAN, JWT_ALGORITHM, JWT_SECRET
from app.Exceptions import (
    InvalidToken,
    InvalidTokenType,
    MissingAuth,
    TokenExpired,
)


class TokenUser:
    """Lightweight user shape from JWT claims. No DB lookup."""
    def __init__(self, user_id: str, plan: str = DEFAULT_PLAN, is_admin: bool = False):
        self.id = user_id
        self.plan = plan
        self.is_admin = is_admin


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
        plan = payload.get("plan")
        return TokenUser(
            user_id=str(sub),
            # Tokens minted before plans existed have no claim → free.
            plan=plan if isinstance(plan, str) and plan else DEFAULT_PLAN,
            is_admin=payload.get("is_admin") is True,
        )