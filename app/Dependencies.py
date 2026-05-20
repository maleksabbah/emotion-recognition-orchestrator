"""
FastAPI dependency factories.

Per-request assembly of repos and services. Same pattern as the gateway:
DB session per request, shared infrastructure (httpx, Redis, Kafka
producer) lives on app.state from main.py's lifespan.

Auth here is JWT-decode-only: orchestrator doesn't have a users table.
The gateway is the issuer; orchestrator only validates and reads `sub`.
"""
from __future__ import annotations

from typing import AsyncIterator

import httpx
import redis.asyncio as redis
from aiokafka import AIOKafkaProducer
from fastapi import Depends, Request
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

def get_http_client(request: Request) -> httpx.AsyncClient:
    return request.app.state.http_client


def get_redis_conn(request: Request) -> redis.Redis:
    return request.app.state.redis


def get_kafka_producer(request: Request) -> AIOKafkaProducer:
    return request.app.state.kafka_producer


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