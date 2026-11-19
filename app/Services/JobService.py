"""
JobService — HTTP-driven operations.

The gateway calls these via the orchestrator's REST API:
  - create_session     POST /api/sessions
  - start_upload_job   POST /api/upload-job
  - list_jobs          GET  /api/sessions
  - get_status         GET  /api/sessions/{id}/status
  - end_session        POST /api/sessions/{id}/end
  - get_usage          GET  /api/usage   (also used by live-mode limits)

Pure DI: takes JobRepository + KafkaProducer.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from app.Config import ACTIVE_JOB_STALE_SECONDS, LIVE_SESSION_CAP_SECONDS

from app.Dtos.JobDto.CreateJobRequest import CreateJobRequest
from app.Dtos.JobDto.JobResponse import JobResponse
from app.Dtos.JobDto.JobStatusResponse import JobStatusResponse
from app.Dtos.JobDto.UploadJobRequest import UploadJobRequest
from app.Dtos.JobDto.UploadJobResponse import UploadJobResponse
from app.Dtos.JobDto.UsageResponse import UsageResponse
from app.Dtos.TaskDto.MediaTask import FrameSource, MediaTask
from app.Entities.Job import Job
from app.Exceptions import Forbidden, NotFound
from app.Repositories.JobRepository import JobRepository
from app.Repositories.KafkaProducer import KafkaProducer


class JobService:
    def __init__(self, jobs: JobRepository, producer: KafkaProducer):
        self.jobs = jobs
        self.producer = producer

    # ══════════════════════════════════════════
    # Session lifecycle
    # ══════════════════════════════════════════

    async def create_session(self, req: CreateJobRequest) -> JobResponse:
        """Pure DB write. Used by both upload and live flows."""
        job = await self.jobs.create(
            mode=req.mode,
            metadata=req.metadata or {},
        )
        return self._to_response(job)

    async def start_upload_job(self, req: UploadJobRequest) -> UploadJobResponse:
        """
        Upload finished:
          1. Persist the real s3_key on the Job row so the burner can fetch
             the source file later. Without this, BurnTask.source_s3_key is
             None and the burner crashes with NoSuchKey.
          2. Record the plan limits the gateway sent in the session metadata.
          3. Publish a MediaTask so the media worker starts processing. Mode
             comes from the session row (set at presign), never the request.
          4. Move the session into 'processing'.
        """
        job = await self.jobs.find_by_id(req.session_id)
        if not job:
            raise NotFound()
        if job.mode not in ("photo", "video"):
            raise Forbidden("Not an upload session")

        # 1. Persist the real S3 key on the session row.
        await self.jobs.set_source_s3_key(req.session_id, req.s3_key)

        # 2. Limits travel with the session (None = unlimited).
        limits = req.limits.model_dump() if req.limits else None
        await self.jobs.set_metadata(
            req.session_id, {**(job.metadata_ or {}), "limits": limits}
        )
        max_video_seconds = limits.get("max_video_seconds") if limits else None

        # 3. Publish MediaTask — media worker pulls source from S3 by this key.
        task = MediaTask(
            task_id=str(uuid.uuid4()),
            session_id=req.session_id,
            mode=job.mode,
            frame_number=0,
            frame_source=FrameSource(type="s3", key=req.s3_key),
            max_video_seconds=max_video_seconds if job.mode == "video" else None,
        )
        await self.producer.publish_media_task(task.model_dump())

        # 4. Status flips to processing for the frontend's progress bar.
        await self.jobs.update_status(req.session_id, "processing")
        return UploadJobResponse(status="queued", session_id=req.session_id)

    async def end_session(self, session_id: str) -> None:
        """
        Best-effort. Called on live WS close (normal, error or limit) and by
        the gateway. Always stamps completed_at so live usage is accurate.
        """
        job = await self.jobs.find_by_id(session_id)
        if not job:
            return
        if job.status not in ("complete", "failed"):
            await self.jobs.mark_complete(session_id)

    # ══════════════════════════════════════════
    # Usage (plan limits)
    # ══════════════════════════════════════════

    async def get_usage(
        self, user_id: str, live_session_cap_seconds: Optional[int] = None
    ) -> UsageResponse:
        """
        This UTC calendar month's usage for one user, computed from session
        rows (nothing stored, nothing drifts). Each live row is capped at
        min(LIVE_SESSION_CAP_SECONDS, live_session_cap_seconds).
        """
        now = datetime.now(timezone.utc)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        active_since = now - timedelta(seconds=ACTIVE_JOB_STALE_SECONDS)

        cap = LIVE_SESSION_CAP_SECONDS
        if live_session_cap_seconds is not None and live_session_cap_seconds > 0:
            cap = min(cap, live_session_cap_seconds)

        counts = await self.jobs.count_usage(user_id, month_start, active_since)
        live_seconds = await self.jobs.sum_live_seconds(user_id, month_start, cap)
        return UsageResponse(
            photos=counts["photos"],
            videos=counts["videos"],
            live_seconds=live_seconds,
            active_jobs=counts["active_jobs"],
        )

    # ══════════════════════════════════════════
    # Reads
    # ══════════════════════════════════════════

    async def list_jobs(self, limit: int = 100, user_id: str | None = None) -> list[JobResponse]:
        rows = await self.jobs.list_all(limit=limit, user_id=user_id)
        return [self._to_response(j) for j in rows]

    async def get_status(self, session_id: str, user_id: str | None = None) -> JobStatusResponse:
        job = await self.jobs.find_by_id(session_id)
        if not job:
            raise NotFound()
        if user_id and job.metadata_.get("user_id") != user_id:
            raise NotFound()

        # Simple progress heuristic. Live + photo are single-frame, so we
        # report 100% when status=complete; video tracks frames_done/total
        # once the burner emits per-frame progress (not modelled here yet).
        progress = 1.0 if job.status == "complete" else 0.0
        return JobStatusResponse(
            session_id=str(job.id),
            status=job.status,
            progress=progress,
            current_frame=job.total_frames,
            total_frames=job.total_frames,
            total_faces=job.total_faces,
            error=(job.metadata_ or {}).get("error") if job.status == "failed" else None,
        )

    # ══════════════════════════════════════════
    # Internal
    # ══════════════════════════════════════════

    @staticmethod
    def _to_response(job: Job) -> JobResponse:
        return JobResponse.model_validate({
            "id": str(job.id),
            "mode": job.mode,
            "status": job.status,
            "created_at": job.created_at,
            "completed_at": job.completed_at,
            "total_frames": job.total_frames,
            "total_faces": job.total_faces,
            "source_s3_key": job.source_s3_key,
            "burned_s3_key": job.burned_s3_key,
        })