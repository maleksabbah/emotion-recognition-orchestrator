"""
PipelineService — Kafka result handlers for the batch flow.

One method per result topic. Each handler is top-down:
  validate → look up job → persist → forward to next stage.

Crops are persisted by the MEDIA WORKER (which calls storage directly).
This service just reads back the s3_keys from MediaResult and writes the
CropRecord rows that link them to detections.
"""
from __future__ import annotations

import logging
import uuid

from app.Dtos.TaskDto.BurnResult import BurnResult
from app.Dtos.TaskDto.InferenceResult import InferenceResult
from app.Dtos.TaskDto.InferenceTask import InferenceTask
from app.Dtos.TaskDto.MediaResult import MediaResult
from app.Entities.CropRecord import CropRecord
from app.Entities.Detection import Detection
from app.Entities.Prediction import Prediction
from app.Repositories.CropRecordRepository import CropRecordRepository
from app.Repositories.DetectionRepository import DetectionRepository
from app.Repositories.JobRepository import JobRepository
from app.Repositories.KafkaProducer import KafkaProducer
from app.Repositories.PredictionRepository import PredictionRepository
from app.Repositories.RedisRepository import RedisRepository

logger = logging.getLogger("orchestrator.pipeline")


class PipelineService:
    def __init__(
        self,
        jobs: JobRepository,
        detections: DetectionRepository,
        predictions: PredictionRepository,
        crops: CropRecordRepository,
        producer: KafkaProducer,
        redis: RedisRepository,
    ):
        self.jobs = jobs
        self.detections = detections
        self.predictions = predictions
        self.crops = crops
        self.producer = producer
        self.redis = redis

    # ══════════════════════════════════════════
    # media_results topic
    # ══════════════════════════════════════════

    async def handle_media_result(self, message: dict) -> None:
        result = MediaResult.model_validate(message)

        job = await self.jobs.find_by_id(result.session_id)
        if not job or job.status in ("complete", "failed"):
            logger.info(
                "Dropping media_result for %s (job missing or terminal)",
                result.session_id,
            )
            return

        # Persist detection rows
        detection_rows = [
            Detection(
                id=uuid.uuid4(),
                session_id=uuid.UUID(result.session_id),
                frame_number=result.frame_number,
                timestamp_ms=result.timestamp_ms,
                face_index=face.face_index,
                bbox_x=face.bbox.x,
                bbox_y=face.bbox.y,
                bbox_w=face.bbox.w,
                bbox_h=face.bbox.h,
                landmark_tier=face.landmark_tier,
                track_id=face.track_id,
            )
            for face in result.faces
        ]
        if detection_rows:
            await self.detections.create_many(detection_rows)

        await self.jobs.increment_counters(
            result.session_id, frames=1, faces=len(result.faces)
        )

        # Persist crop pointers — media worker already uploaded; we just record
        # the links between detection_id, region, and s3_key.
        crop_rows: list[CropRecord] = []
        for face, det in zip(result.faces, detection_rows):
            for region, s3_key in face.crop_s3_keys.items():
                if region not in {"face", "eyes", "mouth", "cheeks", "forehead"}:
                    continue
                crop_rows.append(CropRecord(
                    detection_id=det.id,
                    region=region,
                    s3_key=s3_key,
                ))
        if crop_rows:
            await self.crops.create_many(crop_rows)

        # Fan out InferenceTask per face
        for face in result.faces:
            task = InferenceTask(
                task_id=str(uuid.uuid4()),
                session_id=result.session_id,
                frame_number=result.frame_number,
                face_index=face.face_index,
                track_id=face.track_id,
                bbox=face.bbox,
                face_crop=face.face_crop,
                region_crops=face.region_crops,
            )
            await self.producer.publish_inference_task(task.model_dump())

    # ══════════════════════════════════════════
    # inference_results topic
    # ══════════════════════════════════════════

    async def handle_inference_result(self, message: dict) -> None:
        result = InferenceResult.model_validate(message)

        job = await self.jobs.find_by_id(result.session_id)
        if not job or job.status in ("complete", "failed"):
            return

        detection_id = await self._find_detection_id(
            result.session_id, result.frame_number, result.face_index
        )
        if detection_id is None:
            logger.warning(
                "No detection for session=%s frame=%s face=%s",
                result.session_id, result.frame_number, result.face_index,
            )
            return

        prediction = Prediction(
            detection_id=detection_id,
            emotion_angry=result.emotions.angry,
            emotion_disgust=result.emotions.disgust,
            emotion_fear=result.emotions.fear,
            emotion_happy=result.emotions.happy,
            emotion_neutral=result.emotions.neutral,
            emotion_sad=result.emotions.sad,
            emotion_surprise=result.emotions.surprise,
            top_emotion=result.top_emotion,
            top_confidence=result.top_confidence,
            valence=result.valence,
            arousal=result.arousal,
            intensity=result.intensity,
            inference_time_ms=result.inference_time_ms,
            worker_id=result.worker_id,
        )
        await self.predictions.create(prediction)

        if job.mode == "live":
            await self.redis.publish_live_result(
                result.session_id,
                {
                    "frame_number": result.frame_number,
                    "face_index": result.face_index,
                    "top_emotion": result.top_emotion,
                    "top_confidence": result.top_confidence,
                    "valence": result.valence,
                    "arousal": result.arousal,
                    "intensity": result.intensity,
                },
            )

    # ══════════════════════════════════════════
    # burn_results topic
    # ══════════════════════════════════════════

    async def handle_burn_result(self, message: dict) -> None:
        result = BurnResult.model_validate(message)

        if result.status != "complete":
            logger.error(
                "Burn failed for session %s: %s", result.session_id, result.error
            )
            await self.jobs.update_status(result.session_id, "failed")
            return

        await self.jobs.mark_complete(
            result.session_id, burned_s3_key=result.burned_s3_key
        )

    # ══════════════════════════════════════════
    # Internal
    # ══════════════════════════════════════════

    async def _find_detection_id(
        self, session_id: str, frame_number: int, face_index: int
    ):
        rows = await self.detections.list_for_session(session_id)
        for r in rows:
            if r.frame_number == frame_number and r.face_index == face_index:
                return r.id
        return None






