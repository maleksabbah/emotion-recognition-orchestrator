from __future__ import annotations

from pydantic import BaseModel


class BurnResult(BaseModel):
    """
    Burner → orchestrator on `burn_results` topic.

    The burner POSTs the actual bytes to storage directly, so this message
    only carries the resulting s3_key + status.
    """
    task_id: str
    session_id: str
    status: str            # 'complete' | 'failed'
    burned_s3_key: str | None = None
    file_id: str | None = None
    error: str | None = None
    worker_id: str
    processing_time_ms: float = 0.0