from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class JobResponse(BaseModel):
    """One row from the sessions table. Used by POST /sessions and GET /sessions list."""
    model_config = ConfigDict(from_attributes=True)

    id: str
    mode: str
    status: str
    created_at: datetime
    completed_at: Optional[datetime] = None
    total_frames: int = 0
    total_faces: int = 0
    source_s3_key: Optional[str] = None
    burned_s3_key: Optional[str] = None