from app.Dtos.TaskDto.MediaTask import MediaTask, FrameSource
from app.Dtos.TaskDto.MediaResult import (
    MediaResult,
    DetectedFace,
    RegionCrops,
    Bbox,
)
from app.Dtos.TaskDto.InferenceTask import InferenceTask
from app.Dtos.TaskDto.InferenceResult import InferenceResult, EmotionScores
from app.Dtos.TaskDto.BurnTask import BurnTask, BurnFrame, BurnFacePrediction
from app.Dtos.TaskDto.BurnResult import BurnResult

__all__ = [
    # MediaTask
    "MediaTask", "FrameSource",
    # MediaResult
    "MediaResult", "DetectedFace", "RegionCrops", "Bbox",
    # InferenceTask
    "InferenceTask",
    # InferenceResult
    "InferenceResult", "EmotionScores",
    # BurnTask
    "BurnTask", "BurnFrame", "BurnFacePrediction",
    # BurnResult
    "BurnResult",
]