"""
FastAPI dependency factories.

Per-request assembly of repos and services. Same pattern as the gateway:
DB session per request, shared infrastructure (httpx, Redis, Kafka
producer) lives on app.state from main.py's lifespan.

Auth here is JWT-decode-only: orchestrator doesn't have a users table.
The gateway is the issuer; orchestrator only validates and reads `sub`.

Shared infra getters take `HTTPConnection` (the common parent of `Request`
and `WebSocket`), so the same dependency chain works for both HTTP routes
and the /ws/live WebSocket route. Using `Request` here breaks WS endpoints
because FastAPI injects a `WebSocket` for those, not a `Request`.
"""
from __future__ import annotations

from typing import AsyncIterator

import httpx
import redis.asyncio as redis
from aiokafka import AIOKafkaProducer
from fastapi import Depends
from starlette.requests import HTTPConnection
from sqlalchemy.ext.asyncio import AsyncSession

from app.Config.Database import SessionLocal
from app.Repositories.CropRecordRepository import CropRecordRepository
from app.Repositories.DetectionRepository import DetectionRepository
from app.Repositories.JobRepository import JobRepository
from app.Repositories.KafkaProducer import KafkaProducer
from app.Repositories.PredictionRepository import PredictionRepository
from app.Repositories.RedisRepository import RedisRepository
from app.Repositories.StorageClient import StorageClient
from app.Services.AuthService import AuthService
from app.Services.JobService import JobService
from app.Services.LiveSessionService import LiveSessionService
from app.Services.PipelineService import PipelineService


# ─── Per-request DB session ────────────────────────────────────────────

async def get_db_session() -> AsyncIterator[AsyncSession]:
    session = SessionLocal()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


# ─── Shared infra from app.state ───────────────────────────────────────
# HTTPConnection is the common parent of fastapi.Request and fastapi.WebSocket,
# so the same dep works for both HTTP routes and WS handlers.

def get_http_client(conn: HTTPConnection) -> httpx.AsyncClient:
    return conn.app.state.http_client


def get_redis_conn(conn: HTTPConnection) -> redis.Redis:
    return conn.app.state.redis


def get_kafka_producer(conn: HTTPConnection) -> AIOKafkaProducer:
    return conn.app.state.kafka_producer


# ─── Repositories ──────────────────────────────────────────────────────

def get_job_repo(
    session: AsyncSession = Depends(get_db_session),
) -> JobRepository:
    return JobRepository(session)


def get_detection_repo(
    session: AsyncSession = Depends(get_db_session),
) -> DetectionRepository:
    return DetectionRepository(session)


def get_prediction_repo(
    session: AsyncSession = Depends(get_db_session),
) -> PredictionRepository:
    return PredictionRepository(session)


def get_crop_repo(
    session: AsyncSession = Depends(get_db_session),
) -> CropRecordRepository:
    return CropRecordRepository(session)


def get_redis_repo(
    conn: redis.Redis = Depends(get_redis_conn),
) -> RedisRepository:
    return RedisRepository(conn)


def get_storage_client(
    http: httpx.AsyncClient = Depends(get_http_client),
) -> StorageClient:
    return StorageClient(http)


def get_kafka_producer_wrapper(
    producer: AIOKafkaProducer = Depends(get_kafka_producer),
) -> KafkaProducer:
    return KafkaProducer(producer)


# ─── Services ──────────────────────────────────────────────────────────

def get_auth_service() -> AuthService:
    return AuthService()


def get_job_service(
    jobs: JobRepository = Depends(get_job_repo),
    producer: KafkaProducer = Depends(get_kafka_producer_wrapper),
) -> JobService:
    return JobService(jobs=jobs, producer=producer)


def get_pipeline_service(
    jobs: JobRepository = Depends(get_job_repo),
    detections: DetectionRepository = Depends(get_detection_repo),
    predictions: PredictionRepository = Depends(get_prediction_repo),
    crops: CropRecordRepository = Depends(get_crop_repo),
    producer: KafkaProducer = Depends(get_kafka_producer_wrapper),
    redis_repo: RedisRepository = Depends(get_redis_repo),
) -> PipelineService:
    return PipelineService(
        jobs=jobs,
        detections=detections,
        predictions=predictions,
        crops=crops,
        producer=producer,
        redis=redis_repo,
    )


def get_live_session_service(
    redis_repo: RedisRepository = Depends(get_redis_repo),
    jobs: JobService = Depends(get_job_service),
) -> LiveSessionService:
    return LiveSessionService(redis=redis_repo, jobs=jobs)