"""
ORM models for orchestrator_db.
Sessions, detections, predictions, media files, crop records, worker logs.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Column, DateTime, Double, Enum, ForeignKey,
    Index, Integer, String, Text,
)

from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Session(Base):
    __tablename__ = "sessions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    mode = Column(Enum("live", "video", "photo", name="session_mode"), nullable=False)
    status = Column(
        Enum("active", "processing", "burning", "complete", "failed", name="session_status"),
        nullable=False, default="active",
    )
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    completed_at = Column(DateTime(timezone=True), nullable=True)
    total_frames = Column(Integer, nullable=False, default=0)
    total_faces = Column(Integer, nullable=False, default=0)
    source_s3_key = Column(Text, nullable=True)
    burned_s3_key = Column(Text, nullable=True)
    metadata_ = Column("metadata", JSONB, default=dict)

    detections = relationship("Detection", back_populates="session", cascade="all, delete-orphan")
    worker_logs = relationship("WorkerLog", back_populates="session")


class Detection(Base):
    __tablename__ = "detections"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    session_id = Column(UUID(as_uuid=True), ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False)
    frame_number = Column(Integer, nullable=False)
    timestamp_ms = Column(Double, nullable=False, default=0.0)
    face_index = Column(Integer, nullable=False, default=0)
    bbox_x = Column(Double, nullable=False)
    bbox_y = Column(Double, nullable=False)
    bbox_w = Column(Double, nullable=False)
    bbox_h = Column(Double, nullable=False)
    landmark_tier = Column(Enum("mediapipe", "insightface", "fallback", name="landmark_tier"), nullable=False)
    track_id = Column(Integer, nullable=False, default=0)
    crops_s3_prefix = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    session = relationship("Session", back_populates="detections")
    prediction = relationship("Prediction", back_populates="detection", uselist=False, cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_detections_session_frame", "session_id", "frame_number"),
        Index("idx_detections_track_id", "session_id", "track_id"),
    )


class Prediction(Base):
    __tablename__ = "predictions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    detection_id = Column(UUID(as_uuid=True), ForeignKey("detections.id", ondelete="CASCADE"), nullable=False)
    emotion_angry = Column(Double, nullable=False, default=0.0)
    emotion_disgust = Column(Double, nullable=False, default=0.0)
    emotion_fear = Column(Double, nullable=False, default=0.0)
    emotion_happy = Column(Double, nullable=False, default=0.0)
    emotion_neutral = Column(Double, nullable=False, default=0.0)
    emotion_sad = Column(Double, nullable=False, default=0.0)
    emotion_surprise = Column(Double, nullable=False, default=0.0)
    top_emotion = Column(String(16), nullable=False)
    top_confidence = Column(Double, nullable=False)
    valence = Column(String(16), nullable=False)
    arousal = Column(String(8), nullable=False)
    intensity = Column(String(8), nullable=False)
    inference_time_ms = Column(Double, nullable=True)
    worker_id = Column(String(64), nullable=True)

    detection = relationship("Detection", back_populates="prediction")



class WorkerLog(Base):
    __tablename__ = "worker_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    session_id = Column(UUID(as_uuid=True), ForeignKey("sessions.id", ondelete="SET NULL"), nullable=True)
    worker_id = Column(String(64), nullable=False)
    worker_type = Column(Enum("media", "inference", "burner", name="worker_type"), nullable=False)
    task_type = Column(Enum("frame", "video", "photo", "burn", name="task_type"), nullable=False)
    frames_processed = Column(Integer, nullable=False, default=0)
    processing_time_ms = Column(Double, nullable=False, default=0.0)
    device = Column(String(32), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    session = relationship("Session", back_populates="worker_logs")