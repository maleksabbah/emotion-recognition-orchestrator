"""
AuthService — JWT validation only.

Orchestrator has no users table. The gateway is the issuer of access
tokens; orchestrator only verifies and reads `sub` (user_id) to know
which user is connecting, plus `plan` / `is_admin` for live-mode limits.

Used by the WS route to authenticate the live session — gateway already
authenticates the HTTP routes (internal network, trusted).
"""
from __future__ import annotations

from typing import Any, Optional

import jwt

from app.Config import DEFAULT_PLAN, JWT_ALGORITHM, JWT_SECRET
from app.Exceptions import (
    InvalidToken,
    InvalidTokenType,
    MissingAuth,
    TokenExpired,
)


class TokenUser:
    """
    Lightweight user shape from JWT claims. No DB lookup.

    `live` = {"monthly_seconds", "session_max_seconds"} set by the gateway
    from its plans table, or None when the token doesn't carry it (minted
    before this claim existed) — the caller then uses the LIVE_LIMITS
    fallback for `plan`.
    """
    def __init__(
        self,
        user_id: str,
        plan: str = DEFAULT_PLAN,
        is_admin: bool = False,
        live: Optional[dict[str, int]] = None,
    ):
        self.id = user_id
        self.plan = plan
        self.is_admin = is_admin
        self.live = live


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
            live=_live_claim(payload.get("live")),
        )


def _live_claim(value: Any) -> Optional[dict[str, int]]:
    """Validate the gateway's `live` claim; anything malformed → None (fallback)."""
    if not isinstance(value, dict):
        return None
    try:
        monthly = int(value["monthly_seconds"])
        session_max = int(value["session_max_seconds"])
    except (KeyError, TypeError, ValueError):
        return None
    if monthly < 0 or session_max <= 0:
        return None
    return {"monthly_seconds": monthly, "session_max_seconds": session_max}