"""
Orchestrator entry point.

Builds the FastAPI app with:
  - Lifespan: ensures schema, creates httpx client, Redis connection,
              Kafka producer, spawns 3 Kafka consumer loops
  - CORS, exception handlers
  - Routes: /health, /api/sessions*, /ws/live

Three concurrent background tasks consume Kafka result topics and route
each message through PipelineService.
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.Config import (
    BURN_RESULTS_TOPIC,
    CORS_ORIGINS,
    INFERENCE_RESULTS_TOPIC,
    MEDIA_RESULTS_TOPIC,
    SessionLocal,
    engine,
    get_producer,
    get_redis,
    make_consumer,
)
# Import the Entities package — this registers every model on Base.metadata
# so create_all sees the full table set, not just the few that Routes touch.
from app.Entities import Base  # noqa: F401  (side-effect import)
from app.Exceptions import register_exception_handlers
from app.Repositories.CropRecordRepository import CropRecordRepository
from app.Repositories.DetectionRepository import DetectionRepository
from app.Repositories.JobRepository import JobRepository
from app.Repositories.KafkaProducer import KafkaProducer
from app.Repositories.PredictionRepository import PredictionRepository
from app.Repositories.RedisRepository import RedisRepository
from app.Routes import health_router, job_router, ws_router
from app.Services.PipelineService import PipelineService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("orchestrator")


# ─── Kafka consumer loop ──────────────────────────────────────────────

async def _consume(topic: str, handler) -> None:
    """Pull messages off a topic and dispatch each to the handler."""
    import json

    consumer = make_consumer(topic)
    await consumer.start()
    logger.info("Consuming %s", topic)
    try:
        async for record in consumer:
            try:
                msg = json.loads(record.value.decode("utf-8"))
            except json.JSONDecodeError as e:
                logger.error("Bad JSON from %s: %s", topic, e)
                continue
            try:
                await handler(msg)
            except Exception:
                logger.exception("handler failed on %s", topic)
    finally:
        await consumer.stop()


async def _build_pipeline_per_message(message: dict, topic: str, app: FastAPI) -> None:
    """Each Kafka message gets a fresh DB session + PipelineService instance."""
    async with SessionLocal() as session:
        try:
            pipeline = PipelineService(
                jobs=JobRepository(session),
                detections=DetectionRepository(session),
                predictions=PredictionRepository(session),
                crops=CropRecordRepository(session),
                producer=KafkaProducer(app.state.kafka_producer),
                redis=RedisRepository(app.state.redis),
            )
            if topic == MEDIA_RESULTS_TOPIC:
                await pipeline.handle_media_result(message)
            elif topic == INFERENCE_RESULTS_TOPIC:
                await pipeline.handle_inference_result(message)
            elif topic == BURN_RESULTS_TOPIC:
                await pipeline.handle_burn_result(message)
            await session.commit()
        except Exception:
            await session.rollback()
            raise


# ─── Lifespan ─────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting orchestrator...")

    # Ensure schema exists. Idempotent — create_all only creates missing
    # tables and types, so this is safe to run on every boot. Without this,
    # any model added after the first deploy (e.g. CropRecord) silently
    # has no table, and handle_media_result crashes on INSERT, killing the
    # pipeline before publish_inference_task can run.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Schema ensured")

    # Shared infra
    app.state.http_client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=5.0))
    app.state.redis = get_redis()
    app.state.kafka_producer = get_producer()
    await app.state.kafka_producer.start()
    logger.info("Infra ready")

    # Background Kafka consumer tasks
    consumer_tasks = [
        asyncio.create_task(_consume(
            MEDIA_RESULTS_TOPIC,
            lambda m: _build_pipeline_per_message(m, MEDIA_RESULTS_TOPIC, app),
        )),
        asyncio.create_task(_consume(
            INFERENCE_RESULTS_TOPIC,
            lambda m: _build_pipeline_per_message(m, INFERENCE_RESULTS_TOPIC, app),
        )),
        asyncio.create_task(_consume(
            BURN_RESULTS_TOPIC,
            lambda m: _build_pipeline_per_message(m, BURN_RESULTS_TOPIC, app),
        )),
    ]
    logger.info("Orchestrator ready")

    yield

    logger.info("Shutting down...")
    for t in consumer_tasks:
        t.cancel()
    for t in consumer_tasks:
        try:
            await t
        except asyncio.CancelledError:
            pass

    await app.state.kafka_producer.stop()
    await app.state.redis.close()
    await app.state.http_client.aclose()
    logger.info("Orchestrator stopped")


# ─── App ──────────────────────────────────────────────────────────────

app = FastAPI(
    title="Emotion Recognition Orchestrator",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

register_exception_handlers(app)

app.include_router(health_router)
app.include_router(job_router)
app.include_router(ws_router)