"""
Detection — one face found in one frame of a session/job.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Double, Enum, ForeignKey, Index, Integer, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.Entities.Base import Base


class Detection(Base):
    __tablename__ = "detections"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    session_id = Column(
        UUID(as_uuid=True),
        ForeignKey("sessions.id", ondelete="CASCADE"),
        nullable=False,
    )
    frame_number = Column(Integer, nullable=False)
    timestamp_ms = Column(Double, nullable=False, default=0.0)
    face_index = Column(Integer, nullable=False, default=0)

    # Bounding box
    bbox_x = Column(Double, nullable=False)
    bbox_y = Column(Double, nullable=False)
    bbox_w = Column(Double, nullable=False)
    bbox_h = Column(Double, nullable=False)

    landmark_tier = Column(
        Enum("mediapipe", "insightface", "fallback", name="landmark_tier"),
        nullable=False,
    )
    track_id = Column(Integer, nullable=False, default=0)
    crops_s3_prefix = Column(Text, nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    job = relationship("Job", back_populates="detections")
    prediction = relationship(
        "Prediction",
        back_populates="detection",
        uselist=False,
        cascade="all, delete-orphan",
    )
    crop_records = relationship(
        "CropRecord",
        back_populates="detection",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("idx_detections_session_frame", "session_id", "frame_number"),
        Index("idx_detections_track_id", "session_id", "track_id"),
    )