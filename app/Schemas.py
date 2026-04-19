"""
Shared Pydantic models — contracts between services.
These define what flows through Kafka and Redis.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ──────────────────────────────────────────────
# Enums
# ──────────────────────────────────────────────
class SessionMode(str, Enum):
    LIVE = "live"
    VIDEO = "video"
    PHOTO = "photo"


class SessionStatus(str, Enum):
    ACTIVE = "active"
    PROCESSING = "processing"
    BURNING = "burning"
    COMPLETE = "complete"
    FAILED = "failed"


class LandmarkTier(str, Enum):
    MEDIAPIPE = "mediapipe"
    INSIGHTFACE = "insightface"
    FALLBACK = "fallback"


class FrameSourceType(str, Enum):
    REDIS_CACHE = "redis_cache"
    S3 = "s3"


class MediaSourceType(str, Enum):
    IMAGE = "image"
    VIDEO = "video"
    FRAME_SEQUENCE = "frame_sequence"


class BurnType(str, Enum):
    PHOTO = "photo"
    VIDEO = "video"
    LIVE_EXPORT = "live_export"


# ──────────────────────────────────────────────
# Common
# ──────────────────────────────────────────────
class BBox(BaseModel):
    x: float
    y: float
    w: float
    h: float


class RegionCrops(BaseModel):
    eyes: str   # base64 JPEG
    mouth: str
    cheeks: str
    forehead: str


class EmotionDistribution(BaseModel):
    angry: float = 0.0
    disgust: float = 0.0
    fear: float = 0.0
    happy: float = 0.0
    neutral: float = 0.0
    sad: float = 0.0
    surprise: float = 0.0


# ──────────────────────────────────────────────
# Media processing (Orchestrator ↔ Media Worker)
# ──────────────────────────────────────────────
class FrameSource(BaseModel):
    type: FrameSourceType
    key: str


class MediaConfig(BaseModel):
    face_detection_threshold: float = 0.9
    min_face_size: int = 48
    landmark_tiers: list[str] = ["mediapipe", "insightface", "fallback"]


class MediaTask(BaseModel):
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    session_id: str
    mode: SessionMode
    frame_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    frame_number: int
    timestamp_ms: float = 0.0
    frame_source: FrameSource
    config: MediaConfig = Field(default_factory=MediaConfig)
    priority: int = 1


class DetectedFace(BaseModel):
    face_index: int
    bbox: BBox
    landmark_tier: LandmarkTier
    track_id: int = 0
    face_crop: str      # base64 224x224 JPEG
    region_crops: RegionCrops


class MediaResult(BaseModel):
    task_id: str
    session_id: str
    frame_id: str
    frame_number: int
    timestamp_ms: float
    faces: list[DetectedFace]
    processing_time_ms: float
    worker_id: str


# ──────────────────────────────────────────────
# Inference (Orchestrator ↔ Inference Worker)
# ──────────────────────────────────────────────
class InferenceTask(BaseModel):
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    session_id: str
    detection_id: str
    frame_number: int
    timestamp_ms: float
    face_index: int
    track_id: int = 0
    face_crop: str
    region_crops: RegionCrops
    priority: int = 1


class InferenceResult(BaseModel):
    task_id: str
    session_id: str
    detection_id: str
    frame_number: int
    timestamp_ms: float
    face_index: int
    track_id: int = 0
    bbox: BBox
    emotions: EmotionDistribution
    top_emotion: str
    top_confidence: float
    valence: float
    arousal: float
    intensity: float
    inference_time_ms: float
    worker_id: str


# ──────────────────────────────────────────────
# Burn (Orchestrator ↔ Burner)
# ──────────────────────────────────────────────
class OverlayConfig(BaseModel):
    show_bbox: bool = True
    show_label: bool = True
    show_confidence: bool = True
    show_distribution: bool = False
    font_scale: float = 0.6
    bbox_color: list[int] = [0, 255, 0]
    bbox_thickness: int = 2


class BurnFacePrediction(BaseModel):
    bbox: BBox
    top_emotion: str
    top_confidence: float


class BurnFramePrediction(BaseModel):
    frame_number: int
    timestamp_ms: float
    faces: list[BurnFacePrediction]


class BurnSource(BaseModel):
    type: MediaSourceType
    s3_key: str = ""
    frame_s3_prefix: str = ""


class BurnTask(BaseModel):
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    session_id: str
    burn_type: BurnType
    source: BurnSource
    predictions: list[BurnFramePrediction] = []
    output_s3_key: str
    overlay_config: OverlayConfig = Field(default_factory=OverlayConfig)


class BurnResult(BaseModel):
    task_id: str
    session_id: str
    output_s3_key: str
    size_bytes: int = 0
    processing_time_ms: float
    worker_id: str


# ──────────────────────────────────────────────
# Frontend delivery (Redis → Gateway → WebSocket)
# ──────────────────────────────────────────────
class FrontendFace(BaseModel):
    face_index: int
    track_id: int = 0
    bbox: BBox
    top_emotion: str
    top_confidence: float
    emotions: EmotionDistribution
    valence: str
    arousal: str
    intensity: str


class FrontendFrame(BaseModel):
    session_id: str
    frame_number: int
    timestamp_ms: float
    faces: list[FrontendFace]


# ──────────────────────────────────────────────
# API responses
# ──────────────────────────────────────────────
class SessionResponse(BaseModel):
    id: str
    mode: SessionMode
    status: SessionStatus
    created_at: datetime
    completed_at: Optional[datetime] = None
    total_frames: int = 0
    total_faces: int = 0
    burned_s3_key: Optional[str] = None


class JobStatusResponse(BaseModel):
    session_id: str
    status: SessionStatus
    progress: float = 0.0
    total_frames: int = 0
    current_frame: int = 0
    eta_seconds: Optional[float] = None


class HealthResponse(BaseModel):
    service: str
    status: str = "ok"
    version: str = "1.0.0"
    timestamp: datetime = Field(default_factory=datetime.utcnow)