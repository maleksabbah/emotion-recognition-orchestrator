from __future__ import annotations

from pydantic import BaseModel


class UploadJobResponse(BaseModel):
    """POST /api/upload-job response — orchestrator queued the media task."""
    status: str
    session_id: str