"""
WebSocket route for live mode.

  WS /ws/live?token=<jwt>

Auth: token comes via ?token=... query param (WS handshakes can't carry
custom auth headers from browsers).

Lifecycle:
  - validate token (plan + is_admin come from its claims)
  - plan limits (LiveSessionService.admit): monthly live time left, one
    open session per user. On refusal the socket is accepted and then
    closed with 4002 + reason — closing before accept() would reach the
    browser as a bare handshake failure with no code.
  - accept WS handshake  (BUG fixed: was close() before, dropping the conn)
  - LiveSessionService.open(ws, user_id) + start()
  - loop: handle_frame(bytes) for every incoming binary message, bounded by
    the session's time limit; when it hits, send {"type":"limit",...} and
    close with 4002
  - finally: stop() — even on disconnect / error / limit (stamps completed_at)
"""
from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Depends, Query, WebSocket, WebSocketDisconnect

from app.Config import WS_CLOSE_LIMIT
from app.Exceptions import LiveLimitReached
from app.Services.AuthService import AuthService
from app.Services.LiveSessionService import LiveSessionService
from app.Dependencies import get_auth_service, get_live_session_service

logger = logging.getLogger("orchestrator.ws")

router = APIRouter(tags=["ws"])


@router.websocket("/ws/live")
async def ws_live(
    websocket: WebSocket,
    token: str = Query(...),
    auth: AuthService = Depends(get_auth_service),
    live: LiveSessionService = Depends(get_live_session_service),
) -> None:
    # Validate JWT BEFORE accepting the socket. Reject with close handshake
    # if auth fails — code 1008 = "Policy Violation" is the convention for
    # auth rejection on a WS upgrade.
    try:
        user = await auth.resolve_user_from_token(token)
    except Exception as e:
        logger.warning("WS auth failed: %s", e)
        await websocket.close(code=1008, reason="Unauthorized")
        return

    # Plan limits before the session starts.
    try:
        time_limit = await live.admit(user)
    except LiveLimitReached as e:
        logger.info("Live refused for user %s: %s", user.id, e.detail)
        await websocket.accept()
        await websocket.close(code=WS_CLOSE_LIMIT, reason=e.detail)
        return
    except Exception as e:
        logger.error("Live admission failed for user %s: %s", user.id, e)
        await live.stop()
        await websocket.accept()
        await websocket.close(code=1011, reason="Internal error")
        return

    # Accept the WS handshake. (Previously this was `await websocket.close()`,
    # which killed the connection before any frames could flow — that's why
    # live mode never produced results.)
    await websocket.accept()

    try:
        live.open(websocket, str(user.id))
        await live.start()
        # timeout=None → no limit (admins)
        await asyncio.wait_for(_receive_frames(websocket, live), timeout=time_limit)
    except asyncio.TimeoutError:
        logger.info("Live session %s hit its time limit", live.session_id)
        await live.notify_limit()
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error("WS error: %s", e)
    finally:
        # Don't let a cleanup error escape: the per-connection DB session
        # commits on normal exit, and that commit is what persists the live
        # row's completed_at (= the usage charged for this session).
        try:
            await live.stop()
        except Exception as e:
            logger.error("live.stop failed for %s: %s", live.session_id, e)


async def _receive_frames(websocket: WebSocket, live: LiveSessionService) -> None:
    async for chunk in websocket.iter_bytes():
        await live.handle_frame(chunk)
