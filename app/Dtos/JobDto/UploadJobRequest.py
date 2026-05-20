from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class UploadJobRequest(BaseModel):
    """POST /api/upload-job — gateway signals upload finished, start processing."""
    session_id: str
    mode: Literal["video", "photo"]
    s3_key: str