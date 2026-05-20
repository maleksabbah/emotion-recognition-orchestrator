"""
Prediction — emotion model output for one detection.

Stores all seven softmax probabilities plus argmax label/confidence and
the three secondary heads (valence, arousal, intensity).
"""
from __future__ import annotations

import uuid

from sqlalchemy import Column, Double, ForeignKey, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.Entities.Base import Base


class Prediction(Base):
    __tablename__ = "predictions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    detection_id = Column(
        UUID(as_uuid=True),
        ForeignKey("detections.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Softmax over 7 emotions
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