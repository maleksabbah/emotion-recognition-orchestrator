"""
CropRecord — pointer to one cropped image (one row per region per detection).

Five regions per detection: face, eyes, mouth, cheeks, forehead. Kept in
orchestrator_db so training-data queries can join Detection + Prediction +
CropRecord in one DB without crossing into storage_db.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Enum, ForeignKey, Integer, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.Entities.Base import Base


class CropRecord(Base):
    __tablename__ = "crop_records"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    detection_id = Column(
        UUID(as_uuid=True),
        ForeignKey("detections.id", ondelete="CASCADE"),
        nullable=False,
    )
    region = Column(
        Enum("face", "eyes", "mouth", "cheeks", "forehead", name="crop_region"),
        nullable=False,
    )
    s3_key = Column(Text, nullable=False)
    width = Column(Integer, nullable=True)
    height = Column(Integer, nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    detection = relationship("Detection", back_populates="crop_records")