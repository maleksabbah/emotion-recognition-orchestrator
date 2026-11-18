from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class UploadLimits(BaseModel):
    """Plan limits the gateway forwards for this job. None = unlimited."""
    max_video_seconds: Optional[int] = Field(default=None, gt=0)


class UploadJobRequest(BaseModel):
    """
    POST /api/upload-job — gateway signals upload finished, start processing.

    `mode` is accepted for backward compatibility but the session's stored
    mode (set when the gateway created it at presign) is what's used.
    """
    session_id: str
    mode: Optional[Literal["video", "photo"]] = None
    s3_key: str
    limits: Optional[UploadLimits] = None
