from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class CreateJobRequest(BaseModel):
    """POST /api/sessions — create a session for live/upload/photo modes."""
    mode: Literal["live", "video", "photo"]
    metadata: Optional[dict] = Field(default_factory=dict)