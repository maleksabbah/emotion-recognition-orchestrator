"""
LiveSessionService — owns a single WebSocket connection's lifecycle.

Built once via Depends() with shared infra. The WS route calls admit() to
check plan limits, open() to bind the per-connection bits (websocket,
user_id), then drives start → handle_frame → stop.

  - admit(user)          plan limits: monthly live seconds left, one open
                         session per user. Returns this session's time
                         limit (None = unlimited) or raises LiveLimitReached.
  - start()              create the DB session, send session_created,
                         spin up the results-streaming task
  - handle_frame(bytes)  cache in Redis, publish MediaTask
  - notify_limit()       tell the client the time limit hit, close 4002
  - stop()               cancel stream task, cleanup Redis, end session
                         (stamps completed_at), release the per-user claim
  - _pump_results()      background loop forwarding stream:live entries to WS
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Optional

from fastapi import WebSocket

from app.Config import DEFAULT_PLAN, LIVE_LIMITS, WS_CLOSE_LIMIT, WS_MAX_FRAME_SIZE
from app.Dtos.JobDto.CreateJobRequest import CreateJobRequest
from app.Dtos.TaskDto.MediaTask import FrameSource, MediaTask
from app.Exceptions import LiveLimitReached
from app.Repositories.RedisRepository import RedisRepository
from app.Services.AuthService import TokenUser
from app.Services.JobService import JobService

logger = logging.getLogger("orchestrator.live")


class LiveSessionService:
    def __init__(
        self,
        redis: RedisRepository,
        jobs: JobService,
    ):
        # Shared infrastructure (one set per app)
        self.redis = redis
        self.jobs = jobs

        # Per-connection state (set by open())
        self.ws: Optional[WebSocket] = None
        self.user_id: Optional[str] = None
        self.session_id: Optional[str] = None
        self._frame_number = 0
        self._stream_task: Optional[asyncio.Task] = None
        self._running = False
        self._stopped = False

        # Plan-limit state (set by admit())
        self._claim_user_id: Optional[str] = None
        self._claim_token: Optional[str] = None
        self._limit_reason: str = "Live session limit reached"
        self._limit_detail: str = "Live session limit reached."

    def open(self, websocket: WebSocket, user_id: str) -> "LiveSessionService":
        """Bind this service to one WS connection. Returns self so you can chain."""
        self.ws = websocket
        self.user_id = user_id
        return self

    # ══════════════════════════════════════════
    # Plan limits
    # ══════════════════════════════════════════

    async def admit(self, user: TokenUser) -> Optional[int]:
        """
        Check the user's plan before the session starts. Returns how many
        seconds this session may run (None = unlimited), or raises
        LiveLimitReached with a short reason for the WS close frame.
        """
        if user.is_admin:
            return None

        # The gateway owns the plans and puts this user's live limits in the
        # token. LIVE_LIMITS is only a fallback for tokens without the claim.
        if user.live is not None:
            monthly_seconds = user.live["monthly_seconds"]
            session_max = user.live["session_max_seconds"]
        else:
            limits = LIVE_LIMITS.get(user.plan) or LIVE_LIMITS[DEFAULT_PLAN]
            monthly_seconds = limits["live_minutes_per_month"] * 60
            session_max = limits["live_session_max_seconds"]
        monthly_minutes = monthly_seconds // 60

        usage = await self.jobs.get_usage(user.id, live_session_cap_seconds=session_max)
        remaining = monthly_seconds - usage.live_seconds
        if remaining <= 0:
            raise LiveLimitReached("Monthly live limit reached")

        token = uuid.uuid4().hex
        try:
            claimed = await self.redis.try_claim_live_user(
                user.id, token, ttl_seconds=session_max + 30
            )
        except Exception as e:
            # Redis trouble: don't block on the one-session guard — the time
            # limits below are still enforced in-process.
            logger.error("live claim failed for user %s (allowing): %s", user.id, e)
            claimed, token = True, None
        if not claimed:
            raise LiveLimitReached("Live session already open")
        self._claim_user_id = user.id
        self._claim_token = token

        if remaining < session_max:
            self._limit_reason = "Monthly live limit reached"
            self._limit_detail = (
                f"You've used all {monthly_minutes} live minutes for this month."
            )
            return remaining
        self._limit_reason = "Live session limit reached"
        self._limit_detail = (
            f"Live sessions on your plan are limited to {_duration(session_max)}."
        )
        return session_max

    async def notify_limit(self) -> None:
        """Time limit hit: send a limit message, then close with 4002."""
        if self.ws is None:
            return
        try:
            await self.ws.send_json({
                "type": "limit",
                "code": "live_limit",
                "detail": self._limit_detail,
            })
            await self.ws.close(code=WS_CLOSE_LIMIT, reason=self._limit_reason)
        except Exception as e:
            logger.info("notify_limit on closed socket (%s): %s", self.session_id, e)

    # ══════════════════════════════════════════
    # Lifecycle
    # ══════════════════════════════════════════

    async def start(self) -> None:
        if self.ws is None or self.user_id is None:
            raise RuntimeError("Call open() before start()")

        response = await self.jobs.create_session(
            CreateJobRequest(mode="live", metadata={"user_id": self.user_id})
        )
        self.session_id = response.id

        await self.ws.send_json(
            {"type": "session_created", "session_id": self.session_id}
        )

        self._running = True
        self._stream_task = asyncio.create_task(self._pump_results())
        logger.info(
            "Live session %s started (user=%s)", self.session_id, self.user_id
        )

    async def handle_frame(self, data: bytes) -> None:
        if not self._running or not self.session_id:
            return
        if len(data) > WS_MAX_FRAME_SIZE:
            await self.ws.send_json({"type": "error", "message": "Frame too large"})
            return

        frame_id = RedisRepository.make_frame_id()
        await self.redis.cache_frame(self.session_id, frame_id, data)

        task = MediaTask(
            task_id=str(uuid.uuid4()),
            session_id=self.session_id,
            mode="live",
            frame_number=self._frame_number,
            frame_source=FrameSource(type="redis", key=frame_id),
        )
        await self.redis.enqueue_media_task(self.session_id, task.model_dump())
        self._frame_number += 1

    async def stop(self) -> None:
        """
        Idempotent. Runs even if start() failed part-way, so a created
        session row always gets completed_at and the claim is released.
        """
        if self._stopped:
            return
        self._stopped = True
        self._running = False

        if self._stream_task:
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                pass

        if self.session_id:
            try:
                await self.redis.cleanup_session(self.session_id)
            except Exception as e:
                logger.error("cleanup_session failed for %s: %s", self.session_id, e)
            await self.jobs.end_session(self.session_id)
            logger.info("Live session %s ended", self.session_id)

        if self._claim_user_id and self._claim_token:
            try:
                await self.redis.release_live_user(self._claim_user_id, self._claim_token)
            except Exception as e:
                logger.warning("release_live_user failed for %s: %s", self._claim_user_id, e)
            self._claim_token = None

    # ══════════════════════════════════════════
    # Internal — results pump
    # ══════════════════════════════════════════

    async def _pump_results(self) -> None:
        last_id = "0-0"
        while self._running:
            try:
                payloads, last_id = await self.redis.read_live_stream(
                    self.session_id, last_id=last_id, block_ms=500, count=10
                )
                for payload in payloads:
                    await self.ws.send_json({"type": "result", "data": payload})
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("stream_results error: %s", e)
                await asyncio.sleep(0.5)


def _duration(seconds: int) -> str:
    if seconds % 60 == 0:
        minutes = seconds // 60
        return f"{minutes} minute{'s' if minutes != 1 else ''}"
    return f"{seconds} second{'s' if seconds != 1 else ''}"
