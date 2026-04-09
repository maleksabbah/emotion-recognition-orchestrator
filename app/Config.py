"""
Shared configuration for all services.
Kafka = pipeline backbone (task queues, result queues)
Redis = real-time layer (frame cache, live streams, job status)
PostgreSQL = permanent record
S3/MinIO = file storage
"""
import os


# ──────────────────────────────────────────────
# Kafka
# ──────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Task topics (orchestrator → workers)
TOPIC_MEDIA_TASKS = "media_tasks"
TOPIC_INFERENCE_TASKS = "inference_tasks"
TOPIC_BURN_TASKS = "burn_tasks"

# Result topics (workers → orchestrator)
TOPIC_MEDIA_RESULTS = "media_results"
TOPIC_INFERENCE_RESULTS = "inference_results"
TOPIC_BURN_RESULTS = "burn_results"

# Consumer groups
GROUP_MEDIA_WORKERS = "media-workers"
GROUP_INFERENCE_WORKERS = "inference-workers"
GROUP_BURN_WORKERS = "burn-workers"
GROUP_ORCHESTRATOR = "orchestrator"

KAFKA_NUM_PARTITIONS = int(os.getenv("KAFKA_NUM_PARTITIONS", 6))
KAFKA_REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", 1))


# ──────────────────────────────────────────────
# Redis (real-time layer only)
# ──────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}"

FRAME_CACHE_PREFIX = "frame:"
FRAME_CACHE_TTL = 10
STREAM_LIVE_PREFIX = "stream:live:"
JOB_STATUS_PREFIX = "job:status:"
SESSION_ACTIVE_PREFIX = "session:active:"
SESSION_MAX_DURATION = 3600


# ──────────────────────────────────────────────
# PostgreSQL
# ──────────────────────────────────────────────
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_USER = os.getenv("POSTGRES_USER", "emotion")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "emotion_dev")
ORCHESTRATOR_DB = os.getenv("ORCHESTRATOR_DB", "orchestrator_db")
GATEWAY_DB = os.getenv("GATEWAY_DB", "gateway_db")
ORCHESTRATOR_DB_URL = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{ORCHESTRATOR_DB}"
GATEWAY_DB_URL = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{GATEWAY_DB}"


# ──────────────────────────────────────────────
# S3 / MinIO
# ──────────────────────────────────────────────
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "emotion-recognition")
S3_REGION = os.getenv("S3_REGION", "us-east-1")


# ──────────────────────────────────────────────
# Internal service URLs
# ──────────────────────────────────────────────
ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://orchestrator:8001")
STORAGE_SERVICE_URL = os.getenv("STORAGE_SERVICE_URL", "http://storage-service:8003")

# ──────────────────────────────────────────────
# Priorities (Kafka message headers)
# ──────────────────────────────────────────────
PRIORITY_LIVE = 1
PRIORITY_PHOTO = 2
PRIORITY_VIDEO = 3
PRIORITY_BURN = 4


# ──────────────────────────────────────────────
# Model / detection config
# ──────────────────────────────────────────────
EMOTIONS = ["angry", "disgust", "fear", "happy", "neutral", "sad", "surprise"]
VALENCE_CLASSES = ["negative", "neutral", "positive"]
AROUSAL_CLASSES = ["low", "high"]
INTENSITY_CLASSES = ["low", "medium", "high"]
FACE_INPUT_SIZE = 224
REGION_INPUT_SIZE = 64
FACE_DETECTION_THRESHOLD = 0.9
MIN_FACE_SIZE = 48
LANDMARK_TIERS = ["mediapipe", "insightface", "fallback"]


# ──────────────────────────────────────────────
# Orchestrator tuning
# ──────────────────────────────────────────────
DB_BATCH_SIZE = 30
DB_BATCH_INTERVAL = 2.0
