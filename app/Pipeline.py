"""
Pipeline manager — the orchestrator's brain.

Consumes results from workers via Kafka. At each stage:
  1. Decides whether to proceed
  2. Persists data to PostgreSQL
  3. Routes work to the next stage via Kafka
  4. Delivers results to frontend via Redis

All database writes go through SessionManager (immediate) or BatchWriter (buffered).
"""
from __future__ import annotations
import json
import asyncio
import logging
import uuid
from typing import Optional

import httpx
from aiokafka import AIOKafkaProducer
import redis.asyncio as redis

from app.Config import (
    STORAGE_SERVICE_URL,
    PRIORITY_BURN,
    TOPIC_BURN_TASKS,
    TOPIC_INFERENCE_TASKS,
    TOPIC_MEDIA_TASKS,
    TOPIC_MEDIA_RESULTS,
    TOPIC_INFERENCE_RESULTS,
    TOPIC_BURN_RESULTS,
    GROUP_ORCHESTRATOR,
)
from app.Schemas import (
    BurnFacePrediction,
    BurnFramePrediction,
    BurnSource,
    BurnTask,
    BurnType,
    FrameSource,
    FrameSourceType,
    FrontendFace,
    FrontendFrame,
    InferenceResult,
    InferenceTask,
    MediaResult,
    MediaSourceType,
    MediaTask,
    SessionMode,
)
from app.Kafka import create_consumer, create_producer, publish, extract_priority
from app.Redis import (
    cache_frame,
    is_session_active,
    publish_live,
    set_job_status,
    end_session,
)
from app.Session_manager import SessionManager
from app.Batch_writer import BatchWriter
from app.Database import async_session

logger = logging.getLogger(__name__)


class PipelineManager:
    def __init__(self, redis_conn: redis.Redis):
        self.redis = redis_conn
        self.sessions = SessionManager()
        self.writer = BatchWriter()
        self.producer: Optional[AIOKafkaProducer] = None
        self._consumers: list = []
        self._tasks: list[asyncio.Task] = []
        self._session_predictions: dict[str, list[dict]] = {}

    async def start(self) -> None:
        self.producer = await create_producer()
        await self.writer.start()
        self._tasks = [
            asyncio.create_task(self._consume_media_results()),
            asyncio.create_task(self._consume_inference_results()),
            asyncio.create_task(self._consume_burn_results()),
            asyncio.create_task(self._consume_live_frames()),
        ]
        logger.info("Pipeline manager started")

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for consumer in self._consumers:
            await consumer.stop()
        await self.writer.stop()
        if self.producer:
            await self.producer.stop()
        logger.info("Pipeline manager stopped")

    # ══════════════════════════════════════════
    # ENTRY POINTS (called by API routes)
    # ══════════════════════════════════════════

    async def create_session(self, mode: str, source_s3_key: Optional[str] = None, metadata: Optional[dict] = None):
        async with async_session() as db:
            return await self.sessions.create_session(db, mode, source_s3_key, metadata)

    async def get_session(self, session_id: str):
        async with async_session() as db:
            return await self.sessions.get_session(db, session_id)

    async def list_sessions(self, limit: int = 50):
        async with async_session() as db:
            return await self.sessions.list_sessions(db, limit=limit)

    async def submit_live_frame(
        self, session_id: str, frame_data: bytes, frame_number: int, timestamp_ms: float = 0.0,
    ) -> str:
        frame_id = str(uuid.uuid4())
        await cache_frame(self.redis, session_id, frame_id, frame_data)
        task = MediaTask(
            session_id=session_id,
            mode=SessionMode.LIVE,
            frame_id=frame_id,
            frame_number=frame_number,
            timestamp_ms=timestamp_ms,
            frame_source=FrameSource(type=FrameSourceType.REDIS_CACHE, key=f"frame:{session_id}:{frame_id}"),
            priority=1,
        )
        await publish(self.producer, TOPIC_MEDIA_TASKS, task.model_dump(), key=session_id, priority=1)
        return frame_id

    async def submit_upload_job(
        self, session_id: str, mode: SessionMode, s3_key: str, priority: int = 3,
    ) -> None:
        task = MediaTask(
            session_id=session_id,
            mode=mode,
            frame_number=0,
            frame_source=FrameSource(type=FrameSourceType.S3, key=s3_key),
            priority=priority,
        )
        await publish(self.producer, TOPIC_MEDIA_TASKS, task.model_dump(), key=session_id, priority=priority)
        logger.info(f"Session {session_id} upload queued → {s3_key}")

    # ══════════════════════════════════════════
    # STAGE 1: Media Worker → Orchestrator → Inference Worker
    # ══════════════════════════════════════════

    async def _consume_media_results(self) -> None:
        consumer = await create_consumer(TOPIC_MEDIA_RESULTS, GROUP_ORCHESTRATOR)
        self._consumers.append(consumer)
        try:
            async for message in consumer:
                try:
                    await self._handle_media_result(message.value, extract_priority(message))
                    await consumer.commit()
                except Exception as e:
                    logger.error(f"Media result error: {e}", exc_info=True)
                    await consumer.commit()
        except asyncio.CancelledError:
            pass

    async def _handle_media_result(self, payload: dict, priority: Optional[int]) -> None:
        result = MediaResult(**payload)
        session_id = result.session_id

        # Decision gate: session still valid?
        async with async_session() as db:
            session = await self.sessions.get_session(db, session_id)
        if not session:
            logger.warning(f"Session {session_id} not found, dropping frame")
            return

        if session.mode == "live" and not await is_session_active(self.redis, session_id):
            logger.info(f"Session {session_id} inactive, dropping frame {result.frame_number}")
            return

        # Decision gate: faces detected?
        if not result.faces:
            logger.debug(f"Session {session_id} frame {result.frame_number}: no faces")
            return

        # Update session counters
        async with async_session() as db:
            await self.sessions.increment_counters(db, session_id, frames=1, faces=len(result.faces))

        for face in result.faces:
            s3_prefix = f"crops/{session_id}/{result.frame_id}/"

            # Log detection to PostgreSQL (batched)
            detection_id = await self.writer.add_detection(
                session_id=session_id,
                frame_number=result.frame_number,
                timestamp_ms=result.timestamp_ms,
                face_index=face.face_index,
                bbox=face.bbox.model_dump(),
                landmark_tier=face.landmark_tier.value,
                track_id=face.track_id,
                crops_s3_prefix=s3_prefix,
            )

            # Save crops to S3 (async, non-blocking)
            asyncio.create_task(self._save_crops(session_id, detection_id, result.frame_number, s3_prefix, face))

            # Queue inference
            inference_task = InferenceTask(
                session_id=session_id,
                detection_id=detection_id,
                frame_number=result.frame_number,
                timestamp_ms=result.timestamp_ms,
                face_index=face.face_index,
                track_id=face.track_id,
                face_crop=face.face_crop,
                region_crops=face.region_crops,
                priority=priority or 1,
            )
            await publish(self.producer, TOPIC_INFERENCE_TASKS, inference_task.model_dump(), key=session_id, priority=priority)

        logger.debug(f"Session {session_id} frame {result.frame_number}: {len(result.faces)} faces → inference")

    async def _save_crops(self, session_id: str, detection_id: str, frame_number: int, s3_prefix: str, face) -> None:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                await client.post(
                    f"{STORAGE_SERVICE_URL}/internal/save-crops",
                    json={
                        "session_id": session_id,
                        "detection_id": detection_id,
                        "frame_number": frame_number,
                        "s3_prefix": s3_prefix,
                        "crops": {
                            "face": face.face_crop,
                            "eyes": face.region_crops.eyes,
                            "mouth": face.region_crops.mouth,
                            "cheeks": face.region_crops.cheeks,
                            "forehead": face.region_crops.forehead,
                        },
                    },
                )
        except Exception as e:
            logger.error(f"Crop save failed for {s3_prefix}: {e}")

    # ══════════════════════════════════════════
    # STAGE 2: Inference Worker → Orchestrator → Frontend + Burn
    # ══════════════════════════════════════════

    async def _consume_inference_results(self) -> None:
        consumer = await create_consumer(TOPIC_INFERENCE_RESULTS, GROUP_ORCHESTRATOR)
        self._consumers.append(consumer)
        try:
            async for message in consumer:
                try:
                    await self._handle_inference_result(message.value, extract_priority(message))
                    await consumer.commit()
                except Exception as e:
                    logger.error(f"Inference result error: {e}", exc_info=True)
                    await consumer.commit()
        except asyncio.CancelledError:
            pass

    async def _handle_inference_result(self, payload: dict, priority: Optional[int]) -> None:
        result = InferenceResult(**payload)
        session_id = result.session_id

        # Log prediction to PostgreSQL (batched)
        await self.writer.add_prediction(
            detection_id=result.detection_id,
            emotions=result.emotions.model_dump(),
            top_emotion=result.top_emotion,
            top_confidence=result.top_confidence,
            valence=result.valence,
            arousal=result.arousal,
            intensity=result.intensity,
            inference_time_ms=result.inference_time_ms,
            worker_id=result.worker_id,
        )

        # Build and publish frontend payload via Redis
        frontend_frame = FrontendFrame(
            session_id=session_id,
            frame_number=result.frame_number,
            timestamp_ms=result.timestamp_ms,
            faces=[FrontendFace(
                face_index=result.face_index,
                track_id=result.track_id,
                bbox=result.bbox,
                top_emotion=result.top_emotion,
                top_confidence=result.top_confidence,
                emotions=result.emotions,
                valence=result.valence,
                arousal=result.arousal,
                intensity=result.intensity,
            )],
        )
        await publish_live(self.redis, session_id, frontend_frame.model_dump())

        # Track for burn job
        if session_id not in self._session_predictions:
            self._session_predictions[session_id] = []
        self._session_predictions[session_id].append({
            "frame_number": result.frame_number,
            "timestamp_ms": result.timestamp_ms,
            "faces": [{"bbox": result.bbox.model_dump(), "top_emotion": result.top_emotion, "top_confidence": result.top_confidence}],
        })

        # Update job status
        await set_job_status(self.redis, session_id, status="processing", current_frame=result.frame_number)

        logger.debug(f"Session {session_id} frame {result.frame_number}: {result.top_emotion} ({result.top_confidence:.2f})")

    # ══════════════════════════════════════════
    # STAGE 3: Burner → Orchestrator → Finalize
    # ══════════════════════════════════════════

    async def _consume_burn_results(self) -> None:
        consumer = await create_consumer(TOPIC_BURN_RESULTS, GROUP_ORCHESTRATOR)
        self._consumers.append(consumer)
        try:
            async for message in consumer:
                try:
                    await self._handle_burn_result(message.value)
                    await consumer.commit()
                except Exception as e:
                    logger.error(f"Burn result error: {e}", exc_info=True)
                    await consumer.commit()
        except asyncio.CancelledError:
            pass

    async def _handle_burn_result(self, payload: dict) -> None:
        from app.Schemas import BurnResult
        result = BurnResult(**payload)

        async with async_session() as db:
            await self.sessions.set_burned_key(db, result.session_id, result.output_s3_key)

        await set_job_status(self.redis, result.session_id, status="complete", progress=1.0)
        self._session_predictions.pop(result.session_id, None)
        logger.info(f"Session {result.session_id} complete → {result.output_s3_key}")

    # ══════════════════════════════════════════
    # BURN JOB CREATION
    # ══════════════════════════════════════════

    async def queue_burn_job(
        self, session_id: str, burn_type: BurnType,
        source_s3_key: str = "", source_type: MediaSourceType = MediaSourceType.VIDEO,
    ) -> None:
        predictions = self._session_predictions.get(session_id, [])
        output_ext = "jpg" if burn_type == BurnType.PHOTO else "mp4"

        task = BurnTask(
            session_id=session_id,
            burn_type=burn_type,
            source=BurnSource(type=source_type, s3_key=source_s3_key, frame_s3_prefix=f"crops/{session_id}/"),
            predictions=[
                BurnFramePrediction(
                    frame_number=p["frame_number"],
                    timestamp_ms=p["timestamp_ms"],
                    faces=[BurnFacePrediction(**f) for f in p["faces"]],
                ) for p in predictions
            ],
            output_s3_key=f"outputs/burned/{session_id}/annotated.{output_ext}",
        )

        await publish(self.producer, TOPIC_BURN_TASKS, task.model_dump(), key=session_id, priority=PRIORITY_BURN)

        async with async_session() as db:
            await self.sessions.update_status(db, session_id, "burning")
        await set_job_status(self.redis, session_id, status="burning")
        logger.info(f"Session {session_id} → burn queued")

    # ══════════════════════════════════════════
    # SESSION LIFECYCLE
    # ══════════════════════════════════════════

    async def end_live_session(self, session_id: str) -> None:
        async with async_session() as db:
            session = await self.sessions.get_session(db, session_id)
        if not session:
            return

        await end_session(self.redis, session_id)
        await self.writer.flush()

        await self.queue_burn_job(
            session_id=session_id,
            burn_type=BurnType.LIVE_EXPORT,
            source_type=MediaSourceType.FRAME_SEQUENCE,
        )

    async def _consume_live_frames(self) -> None:
        """Poll Redis for live frames pushed by the gateway."""
        while True:
            try:
                # Check all active sessions for queued frames
                # blpop blocks until a frame is available
                keys = await self.redis.keys("queue:frames:*")
                if not keys:
                    await asyncio.sleep(0.03)
                    continue

                result = await self.redis.blpop(keys, timeout=1)
                if not result:
                    continue

                queue_key, raw = result
                notification = json.loads(raw)
                session_id = notification["session_id"]
                frame_id = notification["frame_id"]
                frame_number = notification["frame_number"]

                # Read frame bytes from cache
                cache_key = f"frame:{session_id}:{frame_id}"
                frame_data = await self.redis.get(cache_key)
                if not frame_data:
                    logger.warning(f"Frame {frame_id} expired from cache")
                    continue

                # Queue to Kafka
                task = MediaTask(
                    session_id=session_id,
                    mode=SessionMode.LIVE,
                    frame_id=frame_id,
                    frame_number=frame_number,
                    timestamp_ms=0.0,
                    frame_source=FrameSource(type=FrameSourceType.REDIS_CACHE, key=cache_key),
                    priority=1,
                )
                await publish(self.producer, TOPIC_MEDIA_TASKS, task.model_dump(), key=session_id, priority=1)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Live frame consumer error: {e}", exc_info=True)
                await asyncio.sleep(0.1)
