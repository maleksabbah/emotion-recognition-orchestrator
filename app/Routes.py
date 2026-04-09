"""
Orchestrator API routes.
Gateway calls these to create sessions, submit frames, check status.
"""
from __future__ import annotations

import base64

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