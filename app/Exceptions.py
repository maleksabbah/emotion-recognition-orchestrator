"""
Domain exceptions + central FastAPI handler.

Services raise these. They never raise HTTPException directly. The handler
registered in main.py converts subclasses to JSON responses.
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

logger = logging.getLogger("orchestrator.exceptions")


# ══════════════════════════════════════════════
# Base
# ══════════════════════════════════════════════

class DomainException(Exception):
    status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR
    default_detail: str = "Internal server error"

    def __init__(self, detail: Optional[str] = None):
        self.detail = detail or self.default_detail
        super().__init__(self.detail)


# ══════════════════════════════════════════════
# 4xx
# ══════════════════════════════════════════════

class Unauthorized(DomainException):
    status_code = status.HTTP_401_UNAUTHORIZED
    default_detail = "Unauthorized"


class MissingAuth(Unauthorized):
    default_detail = "Authentication required"


class InvalidToken(Unauthorized):
    default_detail = "Invalid token"


class TokenExpired(Unauthorized):
    default_detail = "Token expired"


class InvalidTokenType(Unauthorized):
    default_detail = "Invalid token type"


class Forbidden(DomainException):
    status_code = status.HTTP_403_FORBIDDEN
    default_detail = "Forbidden"


class NotFound(DomainException):
    status_code = status.HTTP_404_NOT_FOUND
    default_detail = "Not found"


class LiveLimitReached(Forbidden):
    """Plan limit for live mode. The WS route turns it into close code 4002."""
    default_detail = "Live limit reached"


# ══════════════════════════════════════════════
# 5xx — upstream
# ══════════════════════════════════════════════

class BadGateway(DomainException):
    status_code = status.HTTP_502_BAD_GATEWAY
    default_detail = "Bad gateway"


class StorageUnreachable(BadGateway):
    default_detail = "Storage unreachable"


class StorageError(BadGateway):
    default_detail = "Storage error"


# ══════════════════════════════════════════════
# FastAPI handler
# ══════════════════════════════════════════════

async def domain_exception_handler(
    request: Request, exc: DomainException
) -> JSONResponse:
    if exc.status_code >= 500:
        logger.error(
            "Domain 5xx on %s %s: %s",
            request.method, request.url.path, exc.detail,
        )
    else:
        logger.info(
            "Domain %s on %s %s: %s",
            exc.status_code, request.method, request.url.path, exc.detail,
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
    )


async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catch-all for unhandled exceptions — log the traceback, return 500."""
    logger.exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(DomainException, domain_exception_handler)
    app.add_exception_handler(Exception, generic_exception_handler)