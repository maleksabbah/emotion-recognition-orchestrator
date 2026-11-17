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


# ─── CORS ─────────────────────────────────────────────────────────────

CORS_ORIGINS = os.getenv(
    "CORS_ORIGINS", "http://localhost:3000,http://localhost:5173"
).split(",")