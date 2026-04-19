"""
Orchestrator API routes.
Gateway calls these to create sessions, submit frames, check status.
"""
from __future__ import annotations

import base64
import os

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import Optional

from app.Schemas import (
    BurnType,
    HealthResponse,
    JobStatusResponse,
    MediaSourceType,
    SessionMode,
    SessionStatus,
)
from app.Redis import get_job_status

router = APIRouter()


# ── Request / Response models ────────────────

class CreateSessionRequest(BaseModel):
    mode: SessionMode
    source_s3_key: Optional[str] = None
    metadata: Optional[dict] = None


class CreateSessionResponse(BaseModel):
    session_id: str
    mode: SessionMode
    status: SessionStatus


class SubmitFrameRequest(BaseModel):
    session_id: str
    frame_data: str  # base64 encoded
    frame_number: int
    timestamp_ms: float = 0.0


class SubmitUploadRequest(BaseModel):
    session_id: str
    mode: SessionMode
    s3_key: str


class PresignRequest(BaseModel):
    session_id: str
    filename: str
    content_type: str


class PresignDownloadRequest(BaseModel):
    file_id: str


# ── Routes ───────────────────────────────────

@router.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(service="orchestrator")


@router.post("/sessions", response_model=CreateSessionResponse)
async def create_session(req: CreateSessionRequest, request: Request):
    pipeline = request.app.state.pipeline
    session = await pipeline.create_session(
        mode=req.mode.value,
        source_s3_key=req.source_s3_key,
        metadata=req.metadata,
    )
    if req.mode == SessionMode.LIVE:
        from app.Redis import register_session
        await register_session(pipeline.redis, str(session.id), req.metadata)

    return CreateSessionResponse(
        session_id=str(session.id),
        mode=req.mode,
        status=SessionStatus(session.status),
    )


@router.post("/frames")
async def submit_frame(req: SubmitFrameRequest, request: Request):
    pipeline = request.app.state.pipeline
    try:
        frame_data = base64.b64decode(req.frame_data)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid base64 frame data")

    frame_id = await pipeline.submit_live_frame(
        session_id=req.session_id,
        frame_data=frame_data,
        frame_number=req.frame_number,
        timestamp_ms=req.timestamp_ms,
    )
    return {"frame_id": frame_id}


@router.post("/upload-job")
async def submit_upload_job(req: SubmitUploadRequest, request: Request):
    pipeline = request.app.state.pipeline
    priority = 2 if req.mode == SessionMode.PHOTO else 3
    await pipeline.submit_upload_job(
        session_id=req.session_id,
        mode=req.mode,
        s3_key=req.s3_key,
        priority=priority,
    )
    return {"status": "queued", "session_id": req.session_id}


@router.post("/upload/presign")
async def presign_upload(req: PresignRequest):
    """Proxy presign request to the storage service.

    Storage service already returns browser-reachable URLs (it signs with
    S3_PUBLIC_ENDPOINT), so we just pass them through.
    """
    storage_url = os.getenv("STORAGE_SERVICE_URL", "http://storage:8002")

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{storage_url}/internal/presign/upload",
                json={
                    "session_id": req.session_id,
                    "file_type": "source",
                    "mime_type": req.content_type,
                    "original_filename": req.filename,
                },
            )
            if resp.status_code != 200:
                raise HTTPException(status_code=502, detail=f"Storage error: {resp.text}")
            data = resp.json()

            return {
                "upload_url": data["upload_url"],
                "s3_key": data["s3_key"],
            }
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Storage unreachable: {e}")


@router.get("/sessions/{session_id}/status", response_model=JobStatusResponse)
async def get_session_status(session_id: str, request: Request):
    pipeline = request.app.state.pipeline

    # Redis first (fast path for active jobs)
    status = await get_job_status(pipeline.redis, session_id)
    if status:
        return JobStatusResponse(
            session_id=session_id,
            status=SessionStatus(status.get("status", "processing")),
            progress=float(status.get("progress", 0.0)),
            total_frames=int(status.get("total_frames", 0)),
            current_frame=int(status.get("current_frame", 0)),
            eta_seconds=float(status["eta_seconds"]) if "eta_seconds" in status else None,
        )

    # Fall back to PostgreSQL
    session = await pipeline.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    return JobStatusResponse(
        session_id=session_id,
        status=SessionStatus(session.status),
        total_frames=session.total_frames,
    )


@router.get("/sessions/{session_id}/download")
async def get_session_download(session_id: str, request: Request):
    """Return a presigned download URL for the session's burned output.

    Delegates the signing to the storage service, which uses the public endpoint.
    """
    pipeline = request.app.state.pipeline
    session = await pipeline.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    if not session.burned_s3_key:
        raise HTTPException(status_code=404, detail="No burned output yet")

    # Register the burned file with storage service, then ask it to presign.
    storage_url = os.getenv("STORAGE_SERVICE_URL", "http://storage:8002")
    file_type = "video" if session.burned_s3_key.lower().endswith(".mp4") else "image"
    mime_type = "video/mp4" if file_type == "video" else "image/jpeg"

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Register (idempotent-ish — if it already exists storage will still return a usable record)
            reg = await client.post(
                f"{storage_url}/internal/register",
                json={
                    "session_id": session_id,
                    "category": "burned",
                    "file_type": file_type,
                    "s3_key": session.burned_s3_key,
                    "mime_type": mime_type,
                },
            )
            if reg.status_code != 200:
                raise HTTPException(status_code=502, detail=f"Storage register error: {reg.text}")
            file_id = reg.json()["id"]

            # Ask storage to presign a download
            pres = await client.post(
                f"{storage_url}/internal/presign/download",
                json={"file_id": file_id},
            )
            if pres.status_code != 200:
                raise HTTPException(status_code=502, detail=f"Storage presign error: {pres.text}")
            data = pres.json()

            return {
                "download_url": data["download_url"],
                "s3_key": session.burned_s3_key,
                "file_type": file_type,
            }
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Storage unreachable: {e}")


@router.get("/sessions")
async def list_sessions(request: Request, limit: int = 50):
    pipeline = request.app.state.pipeline
    sessions = await pipeline.list_sessions(limit=limit)
    return [
        {
            "id": str(s.id),
            "mode": s.mode,
            "status": s.status,
            "created_at": s.created_at.isoformat(),
            "total_frames": s.total_frames,
            "total_faces": s.total_faces,
            "burned_s3_key": s.burned_s3_key,
        }
        for s in sessions
    ]


@router.post("/sessions/{session_id}/end")
async def end_session(session_id: str, request: Request):
    pipeline = request.app.state.pipeline
    await pipeline.end_live_session(session_id)
    return {"status": "ending", "session_id": session_id}


@router.post("/sessions/{session_id}/burn")
async def trigger_burn(session_id: str, request: Request):
    pipeline = request.app.state.pipeline
    session = await pipeline.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    burn_map = {"live": BurnType.LIVE_EXPORT, "video": BurnType.VIDEO, "photo": BurnType.PHOTO}
    source_map = {"live": MediaSourceType.FRAME_SEQUENCE, "video": MediaSourceType.VIDEO, "photo": MediaSourceType.IMAGE}

    await pipeline.queue_burn_job(
        session_id=session_id,
        burn_type=burn_map[session.mode],
        source_s3_key=session.source_s3_key or "",
        source_type=source_map[session.mode],
    )
    return {"status": "burn_queued", "session_id": session_id}
