"""
WebSocket route for live mode.

  WS /ws/live?token=<jwt>

Auth: token comes via ?token=... query param (WS handshakes can't carry
custom auth headers from browsers).

Lifecycle:
  - accept
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
    # Validate the JWT BEFORE accepting the socket. Reject by close handshake
    # if auth fails.
    try:
        user = await auth.resolve_user_from_token(token)
    except Exception as e:
        logger.warning("WS auth failed: %s", e)
        await websocket.close(code=1008,reason="Unauthorized")
        return
    await websocket.close()
    live.open(websocket,str(user.id))
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

