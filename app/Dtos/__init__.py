"""
Orchestrator DTOs.

HTTP DTOs:
  JobDto/
    CreateJobRequest, UploadJobRequest,
    JobResponse, UploadJobResponse, JobStatusResponse

Kafka message DTOs (typed Pydantic models for every Kafka payload):
  TaskDto/
    MediaTask, MediaResult, InferenceTask, InferenceResult,
    BurnTask, BurnResult
    + helper sub-models (Bbox, RegionCrops, DetectedFace, EmotionScores,
                         FrameSource, BurnFrame, BurnFacePrediction)
"""
from app.Dtos.JobDto import (
    CreateJobRequest,
    UploadJobRequest,
    JobResponse,
    UploadJobResponse,
    JobStatusResponse,
)
from app.Dtos.TaskDto import (
    MediaTask,
    MediaResult,
    InferenceTask,
    InferenceResult,
    BurnTask,
    BurnResult,
)

__all__ = [
    # HTTP
    "CreateJobRequest",
    "UploadJobRequest",
    "JobResponse",
    "UploadJobResponse",
    "JobStatusResponse",
    # Kafka
    "MediaTask",
    "MediaResult",
    "InferenceTask",
    "InferenceResult",
    "BurnTask",
    "BurnResult",
]