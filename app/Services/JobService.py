"""
JobService — HTTP-driven operations.

The gateway calls these via the orchestrator's REST API:
  - create_session     POST /api/sessions
  - start_upload_job   POST /api/upload-job
  - list_jobs          GET  /api/sessions
  - get_status         GET  /api/sessions/{id}/status
  - end_session        POST /api/sessions/{id}/end

Pure DI: takes JobRepository + KafkaProducer.
"""
from __future__ import annotations

import uuid
from typing import Any

from app.Dtos.JobDto.CreateJobRequest import CreateJobRequest
from app.Dtos.JobDto.JobResponse import JobResponse
from app.Dtos.JobDto.JobStatusResponse import JobStatusResponse
from app.Dtos.JobDto.UploadJobRequest import UploadJobRequest
from app.Dtos.JobDto.UploadJobResponse import UploadJobResponse
from app.Dtos.TaskDto.MediaTask import FrameSource, MediaTask
from app.Entities.Job import Job
from app.Exceptions import NotFound
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
        Upload finished — publish a MediaTask for the source file. The media
        worker picks it up, downloads from MinIO, detects faces, etc.
        """
        job = await self.jobs.find_by_id(req.session_id)
        if not job:
            raise NotFound()

        task = MediaTask(
            task_id=str(uuid.uuid4()),
            session_id=req.session_id,
            mode=req.mode,
            frame_number=0,
            frame_source=FrameSource(type="s3", key=req.s3_key),
        )
        await self.producer.publish_media_task(task.model_dump())

        await self.jobs.update_status(req.session_id, "processing")
        return UploadJobResponse(status="queued", session_id=req.session_id)

    async def end_session(self, session_id: str) -> None:
        """Best-effort. Called by gateway on WS disconnect or admin action."""
        job = await self.jobs.find_by_id(session_id)
        if not job:
            return
        if job.status not in ("complete", "failed"):
            await self.jobs.update_status(session_id, "complete")

    # ══════════════════════════════════════════
    # Reads
    # ══════════════════════════════════════════

    async def list_jobs(self, limit: int = 100) -> list[JobResponse]:
        rows = await self.jobs.list_all(limit=limit)
        return [self._to_response(j) for j in rows]

    async def get_status(self, session_id: str) -> JobStatusResponse:
        job = await self.jobs.find_by_id(session_id)
        if not job:
            raise NotFound()

        # Simple progress heuristic. Live + photo are single-frame, so we report
        # 100% when status=complete; video tracks frames_done/total_frames once
        # the burner has emitted progress (not modelled here yet).
        progress = 1.0 if job.status == "complete" else 0.0
        return JobStatusResponse(
            session_id=str(job.id),
            status=job.status,
            progress=progress,
            current_frame=job.total_frames,
            total_frames=job.total_frames,
            total_faces=job.total_faces,
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