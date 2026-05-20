"""
Orchestrator routes.

  HealthRoutes   GET /health
  JobRoutes      POST/GET /api/sessions*, POST /api/upload-job
  WsRoutes       WS /ws/live?token=...
"""
from app.Routes.HealthRoutes import router as health_router
from app.Routes.JobRoutes import router as job_router
from app.Routes.WsRoutes import router as ws_router

__all__ = ["health_router", "job_router", "ws_router"]