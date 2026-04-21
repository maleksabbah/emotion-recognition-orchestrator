"""
Pipeline manager — the orchestrator's brain.

Consumes results from workers via Kafka. At each stage:
  1. Decides whether to proceed
  2. Persists data to PostgreSQL
  3. Routes work to the next stage via Kafka
  4. Delivers results to frontend via Redis

State-light: the orchestrator never touches MinIO directly. It assigns S3
paths in task messages and storage is the sole gatekeeper of writes.
"""
from __future__ import annotations
import json
import asyncio
import logging
import os
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


# ──────────────────────────────────────────────
# Float → label converters
# ──────────────────────────────────────────────
def _valence_label(v: float) -> str:
    if v < 0.4:
        return "negative"
    if v < 0.6:
        return "neutral"
    return "positive"


def _arousal_label(a: float) -> str:
    return "high" if a >= 0.5 else "low"


def _intensity_label(i: float) -> str:
    if i < 0.34:
        return "low"
    if i < 0.67:
        return "medium"
    return "high"


def _ext_for(source_key: str) -> tuple[str, str, str]:
    """Given a source key (file or prefix), return (task_type, output_ext, output_mime).

    If the key ends with '/', we need to look inside — so we list the prefix
    in MinIO and use the first actual object's extension.
    """
    lower = source_key.lower()
    if lower.endswith("/") or "." not in lower.rsplit("/", 1)[-1]:
        # Prefix — peek inside
        try:
            import boto3
            s3 = boto3.client(
                "s3",
                endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
                aws_access_key_id=os.getenv("S3_ACCESS_KEY", "minioadmin"),
                aws_secret_access_key=os.getenv("S3_SECRET_KEY", "minioadmin"),
            )
            bucket = os.getenv("S3_BUCKET", "emotion-recognition")
            objs = s3.list_objects_v2(Bucket=bucket, Prefix=source_key, MaxKeys=1)
            if "Contents" in objs and objs["Contents"]:
                lower = objs["Contents"][0]["Key"].lower()
        except Exception:
            pass

    if lower.endswith((".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif")):
        return "frame", "jpg", "image/jpeg"
    return "video", "mp4", "video/mp4"


class PipelineManager:
    def __init__(self, redis_conn: redis.Redis):
        self.redis = redis_conn
        self.sessions = SessionManager()
        self.writer = BatchWriter()
        self.producer: Optional[AIOKafkaProducer] = None
        self._consumers: list = []
        self._tasks: list[asyncio.Task] = []
        self._session_predictions: dict[str, dict[int, list[dict]]] = {}
        self._session_expected: dict[str, int] = {}
        self._session_received: dict[str, int] = {}
        self._session_mode: dict[str, str] = {}
        self._session_source_key: dict[str, str] = {}

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
    # ENTRY POINTS
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
        self._session_mode[session_id] = mode.value if hasattr(mode, "value") else str(mode)
        self._session_source_key[session_id] = s3_key

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

        async with async_session() as db:
            session = await self.sessions.get_session(db, session_id)
        if not session:
            logger.warning(f"Session {session_id} not found, dropping frame")
            return

        if session.mode == "live" and not await is_session_active(self.redis, session_id):
            logger.info(f"Session {session_id} inactive, dropping frame {result.frame_number}")
            return

        if not result.faces:
            logger.debug(f"Session {session_id} frame {result.frame_number}: no faces")
            if session.mode in ("video", "photo"):
                await self._complete_upload_session(session_id, no_faces=True)
            return

        async with async_session() as db:
            await self.sessions.increment_counters(db, session_id, frames=1, faces=len(result.faces))

        if session.mode in ("video", "photo"):
            self._session_expected[session_id] = (
                self._session_expected.get(session_id, 0) + len(result.faces)
            )
            self._session_mode.setdefault(session_id, session.mode)

        for face in result.faces:
            s3_prefix = f"crops/{session_id}/{result.frame_id}/"

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

            asyncio.create_task(self._save_crops(
                session_id=session_id,
                frame_number=result.frame_number,
                face_index=face.face_index,
                face=face,
            ))

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

    async def _save_crops(
        self, session_id: str, frame_number: int, face_index: int, face,
    ) -> None:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                await client.post(
                    f"{STORAGE_SERVICE_URL}/internal/save-crops",
                    json={
                        "session_id": session_id,
                        "frame_index": frame_number,
                        "detection_index": face_index,
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
            logger.error(f"Crop save failed for session={session_id} frame={frame_number}: {e}")

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

        valence_label = _valence_label(result.valence)
        arousal_label = _arousal_label(result.arousal)
        intensity_label = _intensity_label(result.intensity)

        await self.writer.add_prediction(
            detection_id=result.detection_id,
            emotions=result.emotions.model_dump(),
            top_emotion=result.top_emotion,
            top_confidence=result.top_confidence,
            valence=valence_label,
            arousal=arousal_label,
            intensity=intensity_label,
            inference_time_ms=result.inference_time_ms,
            worker_id=result.worker_id,
        )

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
                valence=valence_label,
                arousal=arousal_label,
                intensity=intensity_label,
            )],
        )
        await publish_live(self.redis, session_id, frontend_frame.model_dump())

        if session_id not in self._session_predictions:
            self._session_predictions[session_id] = {}
        frame_dets = self._session_predictions[session_id].setdefault(result.frame_number, [])
        bb = result.bbox
        frame_dets.append({
            "bbox": [int(bb.x), int(bb.y), int(bb.x + bb.w), int(bb.y + bb.h)],
            "predictions": {
                "emotion":   {"label": result.top_emotion, "confidence": float(result.top_confidence)},
                "intensity": {"label": intensity_label, "confidence": float(result.intensity)},
                "valence":   {"label": valence_label, "confidence": float(result.valence)},
                "arousal":   {"label": arousal_label, "confidence": float(result.arousal)},
            },
        })

        await set_job_status(self.redis, session_id, status="processing", current_frame=result.frame_number)

        mode = self._session_mode.get(session_id)
        if mode in ("video", "photo"):
            self._session_received[session_id] = self._session_received.get(session_id, 0) + 1
            expected = self._session_expected.get(session_id, 0)
            received = self._session_received[session_id]
            if expected and received >= expected:
                logger.info(f"Session {session_id} all {expected} predictions in → queuing burn")
                await self._complete_upload_session(session_id)

    async def _complete_upload_session(self, session_id: str, no_faces: bool = False) -> None:
        try:
            await self.writer.flush()
        except Exception as e:
            logger.error(f"Flush failed for {session_id}: {e}")

        if no_faces:
            async with async_session() as db:
                await self.sessions.update_status(db, session_id, "complete")
            await set_job_status(self.redis, session_id, status="complete", progress=1.0)
            self._cleanup_session_state(session_id)
            logger.info(f"Session {session_id} complete (no faces)")
            return

        source_key = self._session_source_key.get(session_id, "")
        await self._queue_burn_for_upload(session_id, source_key)

    def _cleanup_session_state(self, session_id: str) -> None:
        self._session_predictions.pop(session_id, None)
        self._session_expected.pop(session_id, None)
        self._session_received.pop(session_id, None)
        self._session_mode.pop(session_id, None)
        self._session_source_key.pop(session_id, None)

    # ══════════════════════════════════════════
    # STAGE 3: Burner → Orchestrator → Finalize
    # ══════════════════════════════════════════

    async def _queue_burn_for_upload(self, session_id: str, source_key: str) -> None:
        """Queue a burn task. Orchestrator assigns output_s3_key; burner reads
        source from MinIO, annotates, hands off to storage for the write.

        No base64, no MinIO touch here — this service stays pure coordination.
        """
        if not source_key:
            logger.error(f"Session {session_id}: no source key, cannot queue burn")
            async with async_session() as db:
                await self.sessions.update_status(db, session_id, "failed")
            await set_job_status(self.redis, session_id, status="failed")
            self._cleanup_session_state(session_id)
            return

        task_type, ext, _mime = _ext_for(source_key)
        output_key = f"outputs/burned/{session_id}/annotated.{ext}"
        preds = self._session_predictions.get(session_id, {})

        if task_type == "frame":
            detections_for_frame = []
            for _, dets in preds.items():
                detections_for_frame.extend(dets)
            task = {
                "session_id": session_id,
                "task_type": "frame",
                "worker_id": "orchestrator",
                "source_s3_key": source_key,
                "output_s3_key": output_key,
                "detections": detections_for_frame,
            }
        else:
            frame_annotations = {str(i): dets for i, dets in preds.items()}
            task = {
                "session_id": session_id,
                "task_type": "video",
                "worker_id": "orchestrator",
                "source_s3_key": source_key,
                "output_s3_key": output_key,
                "frame_annotations": frame_annotations,
            }

        await publish(self.producer, TOPIC_BURN_TASKS, task, key=session_id, priority=PRIORITY_BURN)
        async with async_session() as db:
            await self.sessions.update_status(db, session_id, "burning")
        await set_job_status(self.redis, session_id, status="burning")
        logger.info(f"Session {session_id} → burn queued (task_type={task_type}, output={output_key})")

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
        """Burner wrote the output via storage. We just record + complete.

        Payload from burner:
          { session_id, status, file_id, output_s3_key, processing_ms, error }
        """
        session_id = payload.get("session_id")
        status = payload.get("status")
        file_id = payload.get("file_id")
        output_key = payload.get("output_s3_key")
        err = payload.get("error")

        if not session_id:
            logger.error(f"Burn result missing session_id: {payload}")
            return

        if status != "success" or not output_key:
            logger.error(f"Burn failed for {session_id}: {err}")
            async with async_session() as db:
                await self.sessions.update_status(db, session_id, "failed")
            await set_job_status(self.redis, session_id, status="failed")
            self._cleanup_session_state(session_id)
            return

        async with async_session() as db:
            await self.sessions.set_burned_key(db, session_id, output_key)
            await self.sessions.update_status(db, session_id, "complete")
        await set_job_status(self.redis, session_id, status="complete", progress=1.0)
        self._cleanup_session_state(session_id)
        logger.info(f"Session {session_id} complete → {output_key} (file_id={file_id})")

    # ══════════════════════════════════════════
    # LIVE SESSION LIFECYCLE
    # ══════════════════════════════════════════

    async def end_live_session(self, session_id: str) -> None:
        async with async_session() as db:
            session = await self.sessions.get_session(db, session_id)
        if not session:
            return

        await end_session(self.redis, session_id)
        await self.writer.flush()

        async with async_session() as db:
            await self.sessions.update_status(db, session_id, "complete")
        await set_job_status(self.redis, session_id, status="complete", progress=1.0)
        self._cleanup_session_state(session_id)
        logger.info(f"Live session {session_id} complete")

    async def _consume_live_frames(self) -> None:
        while True:
            try:
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

                cache_key = f"frame:{session_id}:{frame_id}"
                frame_data = await self.redis.get(cache_key)
                if not frame_data:
                    logger.warning(f"Frame {frame_id} expired from cache")
                    continue

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
