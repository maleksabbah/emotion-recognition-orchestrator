from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class Bbox(BaseModel):
    x: float
    y: float
    w: float
    h: float


class RegionCrops(BaseModel):
    """Four 64x64 region crops, base64-encoded JPEG."""
    eyes: str
    mouth: str
    cheeks: str
    forehead: str


class DetectedFace(BaseModel):
    """One face found by the detector — crops already persisted via storage."""
    face_index: int
    track_id: int
    bbox: Bbox
    landmark_tier: Literal["mediapipe", "insightface", "fallback"]
    face_crop: str         # base64 224x224 JPEG
    region_crops: RegionCrops
    # Returned by storage.save_crops in the media worker — region → s3_key
    crop_s3_keys: dict[str, str] = {}


class MediaResult(BaseModel):
    """
    Media worker → orchestrator on `media_results` topic.

    Carries crops inline (base64) so inference can run on them without a
    MinIO round-trip. ~750KB per multi-face frame is acceptable for our
    target throughput.
    """
    task_id: str
    session_id: str
    frame_number: int
    timestamp_ms: float
    faces: list[DetectedFace]
    worker_id: str
    processing_time_ms: float = 0.0
