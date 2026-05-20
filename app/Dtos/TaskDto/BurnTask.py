from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from app.Dtos.TaskDto.MediaResult import Bbox


class BurnFacePrediction(BaseModel):
    """
    Per-face prediction the burner uses to draw one overlay.

    NOTE: the original Mntis code dropped valence/arousal/intensity here and
    the burner silently defaulted them, killing the crosstab word logic.
    They are required now — typing this DTO catches that drift at startup.
    """
    bbox: Bbox
    top_emotion: str
    top_confidence: float
    valence: str
    arousal: str
    intensity: str


class BurnTask(BaseModel):
    """
    Orchestrator → burner on `burn_tasks` topic.

    For video: a full ordered list of frames, each with its detections.
    For photo: a single frame.
    Burner downloads the source from MinIO, draws overlays, saves output.
    """
    task_id: str
    session_id: str
    mode: Literal["video", "photo"]
    source_s3_key: str
    frames: list["BurnFrame"]


class BurnFrame(BaseModel):
    frame_number: int
    timestamp_ms: float
    predictions: list[BurnFacePrediction]


# Resolve forward ref
BurnTask.model_rebuild()