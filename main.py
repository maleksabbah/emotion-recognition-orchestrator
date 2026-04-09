"""
Orchestrator — FastAPI application.

Connects to:
  - Kafka (consumes worker results, publishes tasks)
  - Redis (frame cache, live streams, job status)
  - PostgreSQL (sessions, detections, predictions, crops, worker logs)
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.Config import (
    TOPIC_MEDIA_TASKS, TOPIC_MEDIA_RESULTS,
    TOPIC_INFERENCE_TASKS, TOPIC_INFERENCE_RESULTS,
    TOPIC_BURN_TASKS, TOPIC_BURN_RESULTS,
)
from app.Kafka import ensure_topics
from app.Redis import get_redis
from app.Pipeline import PipelineManager
from app.Routes import router

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("orchestrator")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting orchestrator...")

    # Ensure Kafka topics
    await ensure_topics([
        TOPIC_MEDIA_TASKS, TOPIC_MEDIA_RESULTS,
        TOPIC_INFERENCE_TASKS, TOPIC_INFERENCE_RESULTS,
        TOPIC_BURN_TASKS, TOPIC_BURN_RESULTS,
    ])

    # Connect Redis
    redis_conn = await get_redis()

    # Start pipeline (connects to Kafka + uses PostgreSQL via SQLAlchemy)
    pipeline = PipelineManager(redis_conn=redis_conn)
    await pipeline.start()

    app.state.pipeline = pipeline
    app.state.redis = redis_conn

    logger.info("Orchestrator ready")
    yield

    logger.info("Shutting down...")
    await pipeline.stop()
    await redis_conn.close()
    logger.info("Orchestrator stopped")


app = FastAPI(title="Emotion Recognition Orchestrator", version="1.0.0", lifespan=lifespan)
app.include_router(router, prefix="/api")