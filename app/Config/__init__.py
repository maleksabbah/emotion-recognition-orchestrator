"""
Config package — env vars + external-system factories.

Callers can use either:
    from app.Config import SessionLocal, get_redis, JWT_SECRET
or:
    from app.Config.Database import SessionLocal
"""
from app.Config.Config import (
    # Auth
    JWT_SECRET,
    JWT_ALGORITHM,
    # Postgres
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    ORCHESTRATOR_DB,
    ORCHESTRATOR_DB_URL,
    # Redis
    REDIS_HOST,
    REDIS_PORT,
    REDIS_URL,
    # Downstream
    STORAGE_SERVICE_URL,
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS,
    ORCHESTRATOR_GROUP_ID,
    MEDIA_TASKS_TOPIC,
    INFERENCE_TASKS_TOPIC,
    BURN_TASKS_TOPIC,
    MEDIA_RESULTS_TOPIC,
    INFERENCE_RESULTS_TOPIC,
    BURN_RESULTS_TOPIC,
    # WS
    WS_MAX_FRAME_SIZE,
    # CORS
    CORS_ORIGINS,
)
from app.Config.Database import engine, SessionLocal
from app.Config.Redis import get_redis
from app.Config.Kafka import get_producer, make_consumer

__all__ = [
    # Auth
    "JWT_SECRET",
    "JWT_ALGORITHM",
    # Postgres
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "ORCHESTRATOR_DB",
    "ORCHESTRATOR_DB_URL",
    # Redis
    "REDIS_HOST",
    "REDIS_PORT",
    "REDIS_URL",
    # Downstream
    "STORAGE_SERVICE_URL",
    # Kafka
    "KAFKA_BOOTSTRAP_SERVERS",
    "ORCHESTRATOR_GROUP_ID",
    "MEDIA_TASKS_TOPIC",
    "INFERENCE_TASKS_TOPIC",
    "BURN_TASKS_TOPIC",
    "MEDIA_RESULTS_TOPIC",
    "INFERENCE_RESULTS_TOPIC",
    "BURN_RESULTS_TOPIC",
    # WS
    "WS_MAX_FRAME_SIZE",
    # CORS
    "CORS_ORIGINS",
    # DB
    "engine",
    "SessionLocal",
    # Redis factory
    "get_redis",
    # Kafka factories
    "get_producer",
    "make_consumer",
]