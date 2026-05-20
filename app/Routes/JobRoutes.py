"""
Job routes — HTTP endpoints the gateway calls.

  POST /api/sessions                  create a session (live/video/photo)
  POST /api/upload-job                kick off a batch upload pipeline
  GET  /api/sessions                  list sessions
  GET  /api/sessions/{id}/status      progress snapshot
  POST /api/sessions/{id}/end         end a session (idempotent)
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, status

from app.Dtos.JobDto.CreateJobRequest import CreateJobRequest
from app.Dtos.JobDto.JobResponse import JobResponse
from app.Dtos.JobDto.JobStatusResponse import JobStatusResponse
from app.Dtos.JobDto.UploadJobRequest import UploadJobRequest
from app.Dtos.JobDto.UploadJobResponse import UploadJobResponse
from app.Services.JobService import JobService
from app.Dependencies import get_job_service

router = APIRouter(prefix="/api", tags=["jobs"])


@router.post("/sessions", response_model=JobResponse)
async def create_session(
    req: CreateJobRequest,
    jobs: JobService = Depends(get_job_service),
) -> JobResponse:
    return await jobs.create_session(req)


@router.post("/upload-job", response_model=UploadJobResponse)
async def start_upload_job(
    req: UploadJobRequest,
    jobs: JobService = Depends(get_job_service),
) -> UploadJobResponse:
    return await jobs.start_upload_job(req)


@router.get("/sessions", response_model=list[JobResponse])
async def list_sessions(
    limit: int = 100,
    jobs: JobService = Depends(get_job_service),
) -> list[JobResponse]:
    return await jobs.list_jobs(limit=limit)


@router.get("/sessions/{session_id}/status", response_model=JobStatusResponse)
async def get_status(
    session_id: str,
    jobs: JobService = Depends(get_job_service),
) -> JobStatusResponse:
    return await jobs.get_status(session_id)


@router.post("/sessions/{session_id}/end", status_code=status.HTTP_202_ACCEPTED)
async def end_session(
    session_id: str,
    jobs: JobService = Depends(get_job_service),
) -> dict:
    await jobs.end_session(session_id)
    return {"status": "ended"}