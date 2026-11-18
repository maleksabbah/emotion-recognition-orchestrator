from __future__ import annotations

from pydantic import BaseModel


class UsageResponse(BaseModel):
    """
    GET /api/usage — one user's usage for the current UTC calendar month.

      photos / videos   jobs this month with an uploaded source, not failed
      live_seconds      summed live session time this month (each row capped)
      active_jobs       photo/video jobs currently processing or burning
    """
    photos: int
    videos: int
    live_seconds: int
    active_jobs: int
