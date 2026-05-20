"""
Job — orchestrator's session row. One per upload/live session.

Table name is `sessions` (kept for schema continuity with the original
Mntis code), entity name is `Job` to match the ASR naming.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Enum, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship

from app.Entities.Base import Base


class Job(Base):
    __tablename__ = "sessions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    mode = Column(
        Enum("live", "video", "photo", name="session_mode"),
        nullable=False,
    )
    status = Column(
        Enum(
            "active", "processing", "burning", "complete", "failed",
            name="session_status",
        ),
        nullable=False,
        default="active",
    )
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    completed_at = Column(DateTime(timezone=True), nullable=True)
    total_frames = Column(Integer, nullable=False, default=0)
    total_faces = Column(Integer, nullable=False, default=0)
    source_s3_key = Column(Text, nullable=True)
    burned_s3_key = Column(Text, nullable=True)
    metadata_ = Column("metadata", JSONB, default=dict)

    detections = relationship(
        "Detection",
        back_populates="job",
        cascade="all, delete-orphan",
    )