"""
Redis utilities — real-time layer only.
Frame cache, live stream delivery, job status, session tracking.
Pipeline queues go through Kafka.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Optional

import redis.asyncio as redis

from app.Config import (
    FRAME_CACHE_PREFIX,
    FRAME_CACHE_TTL,
    JOB_STATUS_PREFIX,
    REDIS_URL,
    SESSION_ACTIVE_PREFIX,
    SESSION_MAX_DURATION,
    STREAM_LIVE_PREFIX,
)

logger = logging.getLogger(__name__)


async def get_redis() -> redis.Redis:
    return redis.from_url(REDIS_URL, decode_responses=False)


# ── Frame cache ──────────────────────────────

async def cache_frame(r: redis.Redis, session_id: str, frame_id: str, data: bytes, ttl: int = FRAME_CACHE_TTL) -> None:
    await r.set(f"{FRAME_CACHE_PREFIX}{session_id}:{frame_id}", data, ex=ttl)


async def get_cached_frame(r: redis.Redis, session_id: str, frame_id: str) -> Optional[bytes]:
    return await r.get(f"{FRAME_CACHE_PREFIX}{session_id}:{frame_id}")


# ── Live stream delivery ─────────────────────

async def publish_live(r: redis.Redis, session_id: str, payload: dict[str, Any], max_len: int = 1000) -> str:
    stream = f"{STREAM_LIVE_PREFIX}{session_id}"
    return await r.xadd(stream, {"data": json.dumps(payload).encode("utf-8")}, maxlen=max_len, approximate=True)


async def read_live_stream(r: redis.Redis, session_id: str, last_id: str = "$", block_ms: int = 1000, count: int = 10) -> tuple[list[dict], str]:
    stream = f"{STREAM_LIVE_PREFIX}{session_id}"
    results = []
    new_last_id = last_id
    messages = await r.xread({stream: last_id}, count=count, block=block_ms)
    for _, stream_msgs in messages:
        for msg_id, fields in stream_msgs:
            try:
                results.append(json.loads(fields[b"data"]))
                new_last_id = msg_id
            except (json.JSONDecodeError, KeyError):
                new_last_id = msg_id
    return results, new_last_id


async def cleanup_live_stream(r: redis.Redis, session_id: str) -> None:
    await r.delete(f"{STREAM_LIVE_PREFIX}{session_id}")


# ── Job status ───────────────────────────────

async def set_job_status(r: redis.Redis, session_id: str, **fields) -> None:
    key = f"{JOB_STATUS_PREFIX}{session_id}"
    mapping = {k: str(v) for k, v in fields.items()}
    await r.hset(key, mapping=mapping)
    await r.expire(key, 86400)


async def get_job_status(r: redis.Redis, session_id: str) -> Optional[dict[str, str]]:
    key = f"{JOB_STATUS_PREFIX}{session_id}"
    data = await r.hgetall(key)
    if not data:
        return None
    return {k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v for k, v in data.items()}


# ── Session tracking ─────────────────────────

async def register_session(r: redis.Redis, session_id: str, metadata: Optional[dict] = None) -> None:
    key = f"{SESSION_ACTIVE_PREFIX}{session_id}"
    await r.set(key, json.dumps(metadata or {}).encode(), ex=SESSION_MAX_DURATION)


async def is_session_active(r: redis.Redis, session_id: str) -> bool:
    return await r.exists(f"{SESSION_ACTIVE_PREFIX}{session_id}") > 0


async def end_session(r: redis.Redis, session_id: str) -> None:
    await r.delete(f"{SESSION_ACTIVE_PREFIX}{session_id}")
    await cleanup_live_stream(r, session_id)