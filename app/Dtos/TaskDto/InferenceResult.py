from __future__ import annotations

from pydantic import BaseModel


class EmotionScores(BaseModel):
    """Softmax over 7 emotions — kept as a flat object so DB write is direct."""
    angry: float
    disgust: float
    fear: float
    happy: float
    neutral: float
    sad: float
    surprise: float


class InferenceResult(BaseModel):
    """
    Inference worker → orchestrator on `inference_results` topic.

    All 7 emotion probabilities, plus the model's argmax label/confidence
    and the three secondary heads.
    """
    task_id: str
    session_id: str
    frame_number: int
    face_index: int

    emotions: EmotionScores
    top_emotion: str
    top_confidence: float

    valence: str      # 'positive' | 'neutral' | 'negative'
    arousal: str      # 'low' | 'medium' | 'high'
    intensity: str    # 'low' | 'medium' | 'high'

    inference_time_ms: float = 0.0
    worker_id: str