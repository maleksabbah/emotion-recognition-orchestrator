from __future__ import annotations

from pydantic import BaseModel

from app.Dtos.TaskDto.MediaResult import Bbox, RegionCrops


class InferenceTask(BaseModel):
    """
    Orchestrator → inference worker on `inference_tasks` topic.

    Carries one face's crops. The orchestrator forwards what came in via
    MediaResult — face_crop + region_crops + bbox (so the result can be
    routed back to the right detection row).
    """
    task_id: str
    session_id: str
    frame_number: int
    face_index: int
    track_id: int
    bbox: Bbox
    face_crop: str
    region_crops: RegionCrops