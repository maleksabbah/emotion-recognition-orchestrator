"""
Health check — no auth, for Docker / nginx liveness probes.
"""
from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": "orchestrator"}