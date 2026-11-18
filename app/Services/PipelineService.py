"""
PipelineService — Kafka result handlers for the batch flow.

One method per result topic. Each handler is top-down:
  validate → look up job → persist → forward to next stage.

Crops are persisted by the MEDIA WORKER (which calls storage directly).
This service just reads back the s3_keys from MediaResult and writes the
CropRecord rows that link them to detections.

The full chain:
  media_result   → detections + crops + INFERENCE TASKS published
  inference_result → prediction persisted; if all faces for this session
                     now have predictions, the BURN TASK is published
  burn_result    → session marked complete (or failed)
"""
from __future__ import annotations

import logging
import uuid

from app.Dtos.TaskDto.BurnResult import BurnResult
from app.Dtos.TaskDto.BurnTask import BurnFacePrediction, BurnFrame, BurnTask
from app.Dtos.TaskDto.InferenceResult import InferenceResult
from app.Dtos.TaskDto.InferenceTask import InferenceTask
from app.Dtos.TaskDto.MediaResult import Bbox, MediaResult
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

        # Live mode → push to Redis pub/sub for WS subscribers
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
            # Live sessions never burn — each frame is shipped as it lands.
            return

        # Batch mode (photo / video) → when every detection for the session
        # has a prediction, fan out the burn task. We check this every time
        # because messages arrive out-of-order; only the last one will see
        # the counts match.
        await self._maybe_publish_burn(result.session_id, job)

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
        det = await self.detections.find_by_frame_and_face(
            session_id, frame_number, face_index
        )
        return det.id if det else None

    async def _maybe_publish_burn(self, session_id: str, job) -> None:
        """
        Publish a BurnTask only when every detection has a prediction.

        We rebuild the full per-frame prediction map from DB rather than
        accumulating in memory — the orchestrator may be horizontally
        scaled, and DB is the only shared source of truth.
        """
        dets = await self.detections.list_for_session(session_id)
        if not dets:
            return

        # Batch-fetch all predictions in one query instead of N+1
        det_ids = [det.id for det in dets]
        det_to_pred = await self.predictions.find_for_detections(det_ids)

        # Not all predictions are in yet; another invocation will finish
        if len(det_to_pred) < len(dets):
            return

        # Source key — required by the burner to download the original file.
        # Falls back to a best-effort default if absent so we still attempt
        # something rather than dropping silently.
        source_key = getattr(job, "source_s3_key", None)
        if not source_key:
            logger.warning(
                "No source_s3_key on session %s; burn task will likely fail",
                session_id,
            )
            source_key = f"sessions/{session_id}/source/unknown"

        # Group detections by frame
        frames_map: dict[int, list[tuple[Detection, Prediction]]] = {}
        for det in dets:
            frames_map.setdefault(det.frame_number, []).append(
                (det, det_to_pred[det.id])
            )

        frames: list[BurnFrame] = []
        for frame_number in sorted(frames_map.keys()):
            preds: list[BurnFacePrediction] = []
            ts_ms = 0.0
            for det, pred in frames_map[frame_number]:
                ts_ms = float(det.timestamp_ms or 0.0)
                preds.append(BurnFacePrediction(
                    bbox=Bbox(x=det.bbox_x, y=det.bbox_y, w=det.bbox_w, h=det.bbox_h),
                    top_emotion=pred.top_emotion,
                    top_confidence=float(pred.top_confidence),
                    valence=pred.valence,
                    arousal=pred.arousal,
                    intensity=pred.intensity,
                ))
            frames.append(BurnFrame(
                frame_number=frame_number,
                timestamp_ms=ts_ms,
                predictions=preds,
            ))

        task = BurnTask(
            task_id=str(uuid.uuid4()),
            session_id=session_id,
            mode=job.mode,  # "video" or "photo"
            source_s3_key=source_key,
            frames=frames,
        )
        # Atomically claim the burn — only one inference_result handler wins
        if not await self.jobs.try_set_burning(session_id):
            logger.info("Burn already claimed for session %s, skipping", session_id)
            return

        await self.producer.publish_burn_task(task.model_dump())
        logger.info(
            "Published BurnTask for session %s (%d frames, %d total preds)",
            session_id, len(frames), sum(len(f.predictions) for f in frames),
        )




