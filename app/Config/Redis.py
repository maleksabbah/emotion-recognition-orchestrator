"""
Redis connection factory.
"""
from __future__ import annotations

import redis.asyncio as redis

from app.Config.Config import REDIS_URL


def get_redis() -> redis.Redis:
    return redis.from_url(REDIS_URL, decode_responses=False)