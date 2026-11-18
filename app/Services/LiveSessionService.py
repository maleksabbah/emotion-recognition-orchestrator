"""
LiveSessionService — owns a single WebSocket connection's lifecycle.

Built once via Depends() with shared infra. The WS route calls open() to
bind the per-connection bits (websocket, user_id), then drives start →
handle_frame → stop.

  - start()              create the DB session, send session_created,
                         spin up the results-streaming task
  - handle_frame(bytes)  cache in Redis, publish MediaTask
  - stop()               cancel stream task, cleanup Redis, end session
  - _pump_results()      background loop forwarding stream:live entries to WS
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Optional

from fastapi import WebSocket

from app.Config import WS_MAX_FRAME_SIZE
from app.Dtos.JobDto.CreateJobRequest import CreateJobRequest
from app.Dtos.TaskDto.MediaTask import FrameSource, MediaTask
from app.Repositories.RedisRepository import RedisRepository
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

    def open(self, websocket: WebSocket, user_id: str) -> "LiveSessionService":
        """Bind this service to one WS connection. Returns self so you can chain."""
        self.ws = websocket
        self.user_id = user_id
        return self

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
        if not self._running:
            return
        self._running = False

        if self._stream_task:
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                pass

        if self.session_id:
            await self.redis.cleanup_session(self.session_id)
            await self.jobs.end_session(self.session_id)
            logger.info("Live session %s ended", self.session_id)

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






