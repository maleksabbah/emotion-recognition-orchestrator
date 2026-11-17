from __future__ import annotations

import json
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

_MAX_METADATA_BYTES = 10_240  # 10 KB


class CreateJobRequest(BaseModel):
    """POST /api/sessions — create a session for live/upload/photo modes."""
    mode: Literal["live", "video", "photo"]
    metadata: Optional[dict] = Field(default_factory=dict)

    @field_validator("metadata")
    @classmethod
    def validate_metadata_size(cls, v: Optional[dict]) -> Optional[dict]:
        if v and len(json.dumps(v).encode()) > _MAX_METADATA_BYTES:
            raise ValueError(f"metadata exceeds {_MAX_METADATA_BYTES} bytes")
        return v