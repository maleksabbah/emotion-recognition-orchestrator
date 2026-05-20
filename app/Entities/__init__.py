"""
Orchestrator entities — orchestrator_db tables.

  Job (sessions table)
  Detection
  Prediction
  CropRecord
"""
from app.Entities.Base import Base
from app.Entities.Job import Job
from app.Entities.Detection import Detection
from app.Entities.Prediction import Prediction
from app.Entities.CropRecord import CropRecord

__all__ = ["Base", "Job", "Detection", "Prediction", "CropRecord"]