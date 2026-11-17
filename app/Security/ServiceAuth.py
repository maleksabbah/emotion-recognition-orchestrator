"""
HMAC service-to-service authentication.

sign_request   — adds X-Service-Signature and X-Service-Timestamp headers
verify_request — checks those headers against the shared secret

Fails open when SERVICE_SECRET is empty (local dev).
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
import time

from fastapi import Request
from starlette import status
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

SERVICE_SECRET: str = os.getenv("SERVICE_SECRET", "")

_TIMESTAMP_WINDOW_SECONDS = 30

_HEADER_SIGNATURE = "X-Service-Signature"
_HEADER_TIMESTAMP = "X-Service-Timestamp"


# ── Outbound (clients) ─────────────────────────────────────────────────

def sign_request(headers: dict[str, str]) -> dict[str, str]:
    """
    Add HMAC signature headers to an outbound request dict.
    Returns a *new* dict (never mutates the original).
    """
    if not SERVICE_SECRET:
        return headers

    ts = str(int(time.time()))
    sig = _compute_signature(ts)
    return {
        **headers,
        _HEADER_TIMESTAMP: ts,
        _HEADER_SIGNATURE: sig,
    }


# ── Inbound (routes) ───────────────────────────────────────────────────

async def require_service_auth(request: Request) -> None:
    """
    FastAPI dependency. Rejects requests that fail HMAC verification.
    Fails open when SERVICE_SECRET is empty (local dev / test).
    """
    if not SERVICE_SECRET:
        return

    sig = request.headers.get(_HEADER_SIGNATURE)
    ts = request.headers.get(_HEADER_TIMESTAMP)

    if not sig or not ts:
        logger.warning("Missing service auth headers from %s", request.url.path)
        raise _forbidden()

    # Reject stale timestamps
    try:
        req_time = int(ts)
    except ValueError:
        raise _forbidden()

    if abs(time.time() - req_time) > _TIMESTAMP_WINDOW_SECONDS:
        logger.warning("Stale service timestamp on %s", request.url.path)
        raise _forbidden()

    expected = _compute_signature(ts)
    if not hmac.compare_digest(sig, expected):
        logger.warning("Bad service signature on %s", request.url.path)
        raise _forbidden()


# ── Internal ────────────────────────────────────────────────────────────

def _compute_signature(timestamp: str) -> str:
    return hmac.new(
        SERVICE_SECRET.encode(),
        timestamp.encode(),
        hashlib.sha256,
    ).hexdigest()


def _forbidden():
    from fastapi import HTTPException
    return HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Service authentication failed",
    )
