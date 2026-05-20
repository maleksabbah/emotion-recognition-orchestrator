"""
Orchestrator repositories — boundary layer.

DB-backed:        JobRepository, DetectionRepository,
                  PredictionRepository, CropRecordRepository
Redis-backed:     RedisRepository
HTTP-backed:      StorageClient
Kafka-backed:     KafkaProducer, KafkaConsumer
"""
from app.Repositories.JobRepository import JobRepository
from app.Repositories.DetectionRepository import DetectionRepository
from app.Repositories.PredictionRepository import PredictionRepository
from app.Repositories.CropRecordRepository import CropRecordRepository
from app.Repositories.RedisRepository import RedisRepository
from app.Repositories.StorageClient import StorageClient
from app.Repositories.KafkaProducer import KafkaProducer
from app.Repositories.KafkaConsumer import KafkaConsumer

__all__ = [
    "JobRepository",
    "DetectionRepository",
    "PredictionRepository",
    "CropRecordRepository",
    "RedisRepository",
    "StorageClient",
    "KafkaProducer",
    "KafkaConsumer",
]