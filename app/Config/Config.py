"""
Orchestrator configuration — environment variables loaded once.

Subsystems (Database, Redis, Kafka) read from this module, not from
os.getenv directly. The gateway must use the same JWT_SECRET so tokens
issued by gateway validate here for the WS auth.
"""
from __future__ import annotations

import os


# ─── JWT / Auth (shared with gateway) ──────────────────────────────────

JWT_SECRET = os.environ["JWT_SECRET_KEY"]
JWT_ALGORITHM = "HS256"


# ─── PostgreSQL (orchestrator_db) ──────────────────────────────────────

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "emotion")
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
ORCHESTRATOR_DB = os.getenv("ORCHESTRATOR_DB", "orchestrator_db")
ORCHESTRATOR_DB_URL = os.getenv("DATABASE_URL") or (
    f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{ORCHESTRATOR_DB}"
)


# ─── Redis ─────────────────────────────────────────────────────────────

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_URL = os.getenv("REDIS_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}")


# ─── Downstream services ──────────────────────────────────────────────

STORAGE_SERVICE_URL = os.getenv("STORAGE_SERVICE_URL", "http://storage:8002")


# ─── Kafka (batch flow only) ──────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")
ORCHESTRATOR_GROUP_ID = os.getenv("ORCHESTRATOR_GROUP_ID", "orchestrator")

MEDIA_TASKS_TOPIC = os.getenv("MEDIA_TASKS_TOPIC", "media_tasks")
INFERENCE_TASKS_TOPIC = os.getenv("INFERENCE_TASKS_TOPIC", "inference_tasks")
BURN_TASKS_TOPIC = os.getenv("BURN_TASKS_TOPIC", "burn_tasks")

MEDIA_RESULTS_TOPIC = os.getenv("MEDIA_RESULTS_TOPIC", "media_results")
INFERENCE_RESULTS_TOPIC = os.getenv("INFERENCE_RESULTS_TOPIC", "inference_results")
BURN_RESULTS_TOPIC = os.getenv("BURN_RESULTS_TOPIC", "burn_results")


# ─── WebSocket / Live ─────────────────────────────────────────────────

WS_MAX_FRAME_SIZE = int(os.getenv("WS_MAX_FRAME_SIZE", str(2 * 1024 * 1024)))  # 2 MB


# ─── Plan limits (live mode) ──────────────────────────────────────────
#
# The gateway owns the full plan table (app/Config/Config.py PLANS there).
# The orchestrator enforces live mode on its own WebSocket, so it keeps a
# copy of the live numbers — keep them in sync with the gateway. Plan comes
# from the access token's `plan` claim; unknown/missing → free. Tokens with
# is_admin=true are unlimited.

DEFAULT_PLAN = "free"

LIVE_LIMITS: dict[str, dict[str, int]] = {
    "free": {
        "live_minutes_per_month": int(os.getenv("PLAN_FREE_LIVE_MINUTES_PER_MONTH", "15")),
        "live_session_max_seconds": int(os.getenv("PLAN_FREE_LIVE_SESSION_MAX_SECONDS", "120")),
    },
    "pro": {
        "live_minutes_per_month": int(os.getenv("PLAN_PRO_LIVE_MINUTES_PER_MONTH", "600")),
        "live_session_max_seconds": int(os.getenv("PLAN_PRO_LIVE_SESSION_MAX_SECONDS", "1800")),
    },
}

# WS close code for plan-limit closes (4000-4999 = application-defined).
WS_CLOSE_LIMIT = 4002


# ─── Usage accounting ─────────────────────────────────────────────────

# A live row is charged (completed_at or now) - created_at, capped here so a
# row whose session died without completed_at can't eat a month's quota.
# Callers can pass a tighter cap (the plan's session max).
LIVE_SESSION_CAP_SECONDS = int(os.getenv("LIVE_SESSION_CAP_SECONDS", str(2 * 3600)))

# Photo/video jobs in processing/burning only count as "active" if created
# within this window. Jobs that never reach a terminal status (e.g. a lost
# media result) would otherwise block a user's concurrent-job slot forever.
ACTIVE_JOB_STALE_SECONDS = int(os.getenv("ACTIVE_JOB_STALE_SECONDS", "1800"))


# ─── CORS ─────────────────────────────────────────────────────────────

CORS_ORIGINS = os.getenv(
    "CORS_ORIGINS", "http://localhost:3000,http://localhost:5173"
).split(",")