"""
WebSocket route for live mode.

  WS /ws/live?token=<jwt>

Auth: token comes via ?token=... query param (WS handshakes can't carry
custom auth headers from browsers).

Lifecycle:
  - validate token
  - accept WS handshake  (BUG fixed: was close() before, dropping the conn)
  - LiveSessionService.open(ws, user_id) + start()
  - loop: handle_frame(bytes) for every incoming binary message
  - finally: stop() — even on disconnect / error
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Query, WebSocket, WebSocketDisconnect

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

    # Accept the WS handshake. (Previously this was `await websocket.close()`,
    # which killed the connection before any frames could flow — that's why
    # live mode never produced results.)
    await websocket.accept()

    live.open(websocket, str(user.id))
    await live.start()

    try:
        async for chunk in websocket.iter_bytes():
            await live.handle_frame(chunk)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error("WS error: %s", e)
    finally:
        await live.stop()