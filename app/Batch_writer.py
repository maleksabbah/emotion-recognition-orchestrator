"""
Batch writer — accumulates records and flushes to PostgreSQL in batches.

At 30fps with 3 faces per frame, that's 90 detections + 90 predictions per second.
Instead of 180 INSERTs/sec, we batch and flush every N records or M seconds.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Optional
from app.ORM_Models import Detection, Prediction
from app.Database import async_session
from app.Config import DB_BATCH_INTERVAL, DB_BATCH_SIZE
logger = logging.getLogger(__name__)


class BatchWriter:
    def __init__(self):
        self._detections: list[Detection] = []
        self._predictions: list[Prediction] = []
        self._lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._flush_task = asyncio.create_task(self._periodic_flush())
        logger.info(f"Batch writer started (size={DB_BATCH_SIZE}, interval={DB_BATCH_INTERVAL}s)")

    async def stop(self) -> None:
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        await self.flush()
        logger.info("Batch writer stopped")

    async def add_detection(
        self, session_id: str, frame_number: int, timestamp_ms: float,
        face_index: int, bbox: dict, landmark_tier: str, track_id: int = 0,
        crops_s3_prefix: Optional[str] = None,
    ) -> str:
        detection_id = uuid.uuid4()
        record = Detection(
            id=detection_id,
            session_id=uuid.UUID(session_id),
            frame_number=frame_number,
            timestamp_ms=timestamp_ms,
            face_index=face_index,
            bbox_x=bbox["x"],
            bbox_y=bbox["y"],
            bbox_w=bbox["w"],
            bbox_h=bbox["h"],
            landmark_tier=landmark_tier,
            track_id=track_id,
            crops_s3_prefix=crops_s3_prefix,
        )
        async with self._lock:
            self._detections.append(record)
            if len(self._detections) >= DB_BATCH_SIZE:
                await self._flush_detections()
        return str(detection_id)

    async def add_prediction(
        self, detection_id: str, emotions: dict, top_emotion: str,
        top_confidence: float, valence: str, arousal: str, intensity: str,
        inference_time_ms: float = 0.0, worker_id: str = "",
    ) -> str:
        prediction_id = uuid.uuid4()
        record = Prediction(
            id=prediction_id,
            detection_id=uuid.UUID(detection_id),
            emotion_angry=emotions.get("angry", 0.0),
            emotion_disgust=emotions.get("disgust", 0.0),
            emotion_fear=emotions.get("fear", 0.0),
            emotion_happy=emotions.get("happy", 0.0),
            emotion_neutral=emotions.get("neutral", 0.0),
            emotion_sad=emotions.get("sad", 0.0),
            emotion_surprise=emotions.get("surprise", 0.0),
            top_emotion=top_emotion,
            top_confidence=top_confidence,
            valence=valence,
            arousal=arousal,
            intensity=intensity,
            inference_time_ms=inference_time_ms,
            worker_id=worker_id,
        )
        async with self._lock:
            self._predictions.append(record)
            if len(self._predictions) >= DB_BATCH_SIZE:
                await self._flush_predictions()
        return str(prediction_id)




    async def flush(self) -> None:
        async with self._lock:
            await self._flush_detections()
            await self._flush_predictions()

    async def _flush_detections(self) -> None:
        if not self._detections:
            return
        records = self._detections[:]
        self._detections.clear()
        try:
            async with async_session() as db:
                db.add_all(records)
                await db.commit()
            logger.debug(f"Flushed {len(records)} detections")
        except Exception as e:
            logger.error(f"Failed to flush detections: {e}", exc_info=True)
            self._detections.extend(records)

    async def _flush_predictions(self) -> None:
        if not self._predictions:
            return
        records = self._predictions[:]
        self._predictions.clear()
        try:
            async with async_session() as db:
                db.add_all(records)
                await db.commit()
            logger.debug(f"Flushed {len(records)} predictions")
        except Exception as e:
            logger.error(f"Failed to flush predictions: {e}", exc_info=True)
            self._predictions.extend(records)


    async def _periodic_flush(self) -> None:
        while True:
            await asyncio.sleep(DB_BATCH_INTERVAL)
            await self.flush()