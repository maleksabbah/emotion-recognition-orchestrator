"""
RedisRepository — orchestrator's Redis access.

Used for live mode:
  - cache_frame:                browser frame bytes parked here until media reads it
  - dequeue_frame_notification: orchestrator pulls the next pending frame ref
  - publish_live_result:        push a per-frame result onto the per-session stream

Storage of frame bytes in Redis is intentional — workers fetch by frame_id
out of the same Redis, avoiding a MinIO round-trip for live mode.
"""
from __future__ import annotations

import json
import uuid
from typing import Optional

import redis.asyncio as redis


_FRAME_PREFIX = "frame"           # frame:{session_id}:{frame_id} → bytes
_QUEUE_PREFIX = "queue:frames"    # queue:frames:{session_id} → list of JSON refs
_STREAM_PREFIX = "stream:live"    # stream:live:{session_id} → XADD entries
_FRAME_TTL_SECONDS = 30


class RedisRepository:
    def __init__(self, conn: redis.Redis):
        self.r = conn

    # ══════════════════════════════════════════
    # Frame cache (WS handler stages bytes here)
    # ══════════════════════════════════════════

    async def cache_frame(
        self, session_id: str, frame_id: str, data: bytes
    ) -> None:
        await self.r.set(
            self._frame_key(session_id, frame_id),
            data,
            ex=_FRAME_TTL_SECONDS,
        )

    async def fetch_frame(self, session_id: str, frame_id: str) -> Optional[bytes]:
        return await self.r.get(self._frame_key(session_id, frame_id))

    async def delete_frame(self, session_id: str, frame_id: str) -> None:
        await self.r.delete(self._frame_key(session_id, frame_id))

    # ══════════════════════════════════════════
    # Live task queue (one list per session)
    # The worker pops MediaTask dicts off this list. The dict includes the
    # frame_source key, which the worker uses to fetch_frame() the bytes.
    # ══════════════════════════════════════════

    async def enqueue_media_task(self, session_id: str, task: dict) -> None:
        await self.r.rpush(
            self._queue_key(session_id),
            json.dumps(task),
        )

    async def dequeue_media_task(
        self, session_id: str, block_seconds: float = 0.5
    ) -> Optional[dict]:
        """Pop one media task or return None after the timeout."""
        raw = await self.r.blpop(
            self._queue_key(session_id), timeout=block_seconds
        )
        if raw is None:
            return None
        _, payload = raw
        return json.loads(payload)

    # ══════════════════════════════════════════
    # Live result stream (one stream per session)
    # ══════════════════════════════════════════

    async def publish_live_result(self, session_id: str, payload: dict) -> str:
        return await self.r.xadd(
            self._stream_key(session_id),
            {"data": json.dumps(payload)},
        )

    async def read_live_stream(
        self,
        session_id: str,
        last_id: str = "0-0",
        block_ms: int = 500,
        count: int = 10,
    ) -> tuple[list[dict], str]:
        """Returns (parsed payloads, new last_id)."""
        result = await self.r.xread(
            {self._stream_key(session_id): last_id},
            block=block_ms,
            count=count,
        )
        if not result:
            return [], last_id

        payloads: list[dict] = []
        new_last_id = last_id
        for _stream_name, entries in result:
            for entry_id, fields in entries:
                new_last_id = (
                    entry_id.decode() if isinstance(entry_id, bytes) else entry_id
                )
                raw = fields.get(b"data") or fields.get("data")
                if raw is None:
                    continue
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                payloads.append(json.loads(raw))
        return payloads, new_last_id

    # ══════════════════════════════════════════
    # Cleanup on session end
    # ══════════════════════════════════════════

    async def cleanup_session(self, session_id: str) -> None:
        """Drop the per-session queue and stream. Frames TTL out on their own."""
        await self.r.delete(
            self._queue_key(session_id),
            self._stream_key(session_id),
        )

    # ══════════════════════════════════════════
    # Helpers
    # ══════════════════════════════════════════

    @staticmethod
    def _frame_key(session_id: str, frame_id: str) -> str:
        return f"{_FRAME_PREFIX}:{session_id}:{frame_id}"

    @staticmethod
    def _queue_key(session_id: str) -> str:
        return f"{_QUEUE_PREFIX}:{session_id}"

    @staticmethod
    def _stream_key(session_id: str) -> str:
        return f"{_STREAM_PREFIX}:{session_id}"

    @staticmethod
    def make_frame_id() -> str:
        return uuid.uuid4().hex




