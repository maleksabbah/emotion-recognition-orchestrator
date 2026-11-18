from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel


class FrameSource(BaseModel):
    """Where the worker fetches the bytes from."""
    type: Literal["s3", "redis"]
    key: str  # s3_key for type=s3, frame_id for type=redis


class MediaTask(BaseModel):
    """
    Orchestrator → media worker on `media_tasks` Kafka topic.

    Tells the worker which frame to process and where to find its bytes.
    Workers GET bytes directly (boto3 for s3, redis.get for live frames).
    """
    task_id: str
    session_id: str
    mode: Literal["live", "video", "photo"]
    frame_number: int
    frame_source: FrameSource
    timestamp_ms: float = 0.0