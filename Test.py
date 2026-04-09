"""
Unit tests for the orchestrator.
Mocks Kafka, Redis, and PostgreSQL so tests run without any infrastructure.
"""
import uuid
import base64
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.Schemas import (
    BBox,
    BurnType,
    DetectedFace,
    EmotionDistribution,
    InferenceResult,
    LandmarkTier,
    MediaResult,
    MediaSourceType,
    RegionCrops,
    SessionMode,
)


@pytest.fixture
def mock_redis():
    r = AsyncMock()
    r.set = AsyncMock()
    r.get = AsyncMock(return_value=None)
    r.exists = AsyncMock(return_value=1)
    r.xadd = AsyncMock(return_value=b"1234-0")
    r.hset = AsyncMock()
    r.hgetall = AsyncMock(return_value={})
    r.expire = AsyncMock()
    r.delete = AsyncMock()
    r.close = AsyncMock()
    return r


@pytest.fixture
def mock_producer():
    producer = AsyncMock()
    producer.send_and_wait = AsyncMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    return producer


@pytest.fixture
def fake_session():
    s = MagicMock()
    s.id = uuid.uuid4()
    s.mode = "live"
    s.status = "active"
    s.created_at = datetime.now(timezone.utc)
    s.completed_at = None
    s.total_frames = 0
    s.total_faces = 0
    s.source_s3_key = None
    s.burned_s3_key = None
    return s


@pytest.fixture
def sample_crops():
    return RegionCrops(
        eyes="base64eyes",
        mouth="base64mouth",
        cheeks="base64cheeks",
        forehead="base64forehead",
    )


@pytest.fixture
def sample_face(sample_crops):
    return DetectedFace(
        face_index=0,
        bbox=BBox(x=0.1, y=0.1, w=0.3, h=0.4),
        landmark_tier=LandmarkTier.MEDIAPIPE,
        track_id=1,
        face_crop="base64face",
        region_crops=sample_crops,
    )


@pytest.fixture
def sample_media_result(sample_face):
    return MediaResult(
        task_id=str(uuid.uuid4()),
        session_id=str(uuid.uuid4()),
        frame_id=str(uuid.uuid4()),
        frame_number=1,
        timestamp_ms=33.3,
        faces=[sample_face],
        processing_time_ms=15.2,
        worker_id="media-worker-0",
    )


@pytest.fixture
def sample_inference_result():
    return InferenceResult(
        task_id=str(uuid.uuid4()),
        session_id=str(uuid.uuid4()),
        detection_id=str(uuid.uuid4()),
        frame_number=1,
        timestamp_ms=33.3,
        face_index=0,
        track_id=1,
        bbox=BBox(x=0.1, y=0.1, w=0.3, h=0.4),
        emotions=EmotionDistribution(
            angry=0.02, disgust=0.01, fear=0.03,
            happy=0.85, neutral=0.05, sad=0.02, surprise=0.02,
        ),
        top_emotion="happy",
        top_confidence=0.85,
        valence="positive",
        arousal="high",
        intensity="high",
        inference_time_ms=23.4,
        worker_id="inference-worker-gpu-0",
    )


@pytest.fixture
def mock_pipeline(mock_redis, fake_session, mock_producer):
    """A fully mocked pipeline manager."""
    pipeline = AsyncMock()
    pipeline.create_session = AsyncMock(return_value=fake_session)
    pipeline.get_session = AsyncMock(return_value=fake_session)
    pipeline.list_sessions = AsyncMock(return_value=[fake_session])
    pipeline.submit_live_frame = AsyncMock(return_value="frame-id-123")
    pipeline.submit_upload_job = AsyncMock()
    pipeline.end_live_session = AsyncMock()
    pipeline.queue_burn_job = AsyncMock()
    pipeline.redis = mock_redis
    pipeline.start = AsyncMock()
    pipeline.stop = AsyncMock()
    return pipeline


@pytest.fixture
def test_client(mock_redis, mock_pipeline):
    """
    Create a TestClient using the real main.py app,
    but with Kafka, Redis, and PipelineManager all mocked.
    The real lifespan runs — proving the full wiring works.
    """
    from fastapi.testclient import TestClient

    with patch("app.main.ensure_topics", new_callable=AsyncMock), \
         patch("app.main.get_redis", new_callable=AsyncMock, return_value=mock_redis), \
         patch("app.main.PipelineManager", return_value=mock_pipeline):
        from main import app
        with TestClient(app) as c:
            yield c


# ══════════════════════════════════════════════
# Schema tests
# ══════════════════════════════════════════════

class TestSchemas:
    def test_media_result_deserialize(self, sample_media_result):
        data = sample_media_result.model_dump()
        parsed = MediaResult(**data)
        assert parsed.session_id == sample_media_result.session_id
        assert len(parsed.faces) == 1
        assert parsed.faces[0].landmark_tier == LandmarkTier.MEDIAPIPE

    def test_inference_result_deserialize(self, sample_inference_result):
        data = sample_inference_result.model_dump()
        parsed = InferenceResult(**data)
        assert parsed.top_emotion == "happy"
        assert parsed.top_confidence == 0.85
        assert parsed.emotions.happy == 0.85

    def test_emotion_distribution_sums(self, sample_inference_result):
        emotions = sample_inference_result.emotions
        total = (emotions.angry + emotions.disgust + emotions.fear +
                 emotions.happy + emotions.neutral + emotions.sad + emotions.surprise)
        assert abs(total - 1.0) < 0.01

    def test_bbox_values(self):
        bbox = BBox(x=0.12, y=0.08, w=0.25, h=0.32)
        assert 0 <= bbox.x <= 1
        assert 0 <= bbox.y <= 1
        assert bbox.w > 0
        assert bbox.h > 0

    def test_media_result_no_faces(self):
        result = MediaResult(
            task_id="t1", session_id="s1", frame_id="f1",
            frame_number=0, timestamp_ms=0.0,
            faces=[], processing_time_ms=5.0, worker_id="w1",
        )
        assert len(result.faces) == 0

    def test_region_crops_all_present(self, sample_crops):
        assert sample_crops.eyes
        assert sample_crops.mouth
        assert sample_crops.cheeks
        assert sample_crops.forehead


# ══════════════════════════════════════════════
# Batch writer tests
# ══════════════════════════════════════════════

class TestBatchWriter:
    @pytest.mark.asyncio
    async def test_add_detection_returns_uuid(self):
        from app.Batch_writer import BatchWriter
        writer = BatchWriter()
        detection_id = await writer.add_detection(
            session_id=str(uuid.uuid4()),
            frame_number=1,
            timestamp_ms=33.3,
            face_index=0,
            bbox={"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4},
            landmark_tier="mediapipe",
            track_id=1,
        )
        assert detection_id is not None
        uuid.UUID(detection_id)

    @pytest.mark.asyncio
    async def test_add_prediction_returns_uuid(self):
        from app.Batch_writer import BatchWriter
        writer = BatchWriter()
        prediction_id = await writer.add_prediction(
            detection_id=str(uuid.uuid4()),
            emotions={"angry": 0.1, "happy": 0.9},
            top_emotion="happy",
            top_confidence=0.9,
            valence="positive",
            arousal="high",
            intensity="high",
        )
        assert prediction_id is not None
        uuid.UUID(prediction_id)

    @pytest.mark.asyncio
    async def test_buffer_accumulates(self):
        from app.Batch_writer import BatchWriter
        writer = BatchWriter()
        for i in range(5):
            await writer.add_detection(
                session_id=str(uuid.uuid4()),
                frame_number=i,
                timestamp_ms=float(i * 33),
                face_index=0,
                bbox={"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4},
                landmark_tier="mediapipe",
            )
        assert len(writer._detections) == 5

    @pytest.mark.asyncio
    @patch("app.Batch_writer.async_session")
    async def test_flush_clears_buffer(self, mock_session_factory):
        from app.Batch_writer import BatchWriter

        mock_db = AsyncMock()
        mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=False)

        writer = BatchWriter()
        await writer.add_detection(
            session_id=str(uuid.uuid4()),
            frame_number=0, timestamp_ms=0.0, face_index=0,
            bbox={"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4},
            landmark_tier="mediapipe",
        )
        assert len(writer._detections) == 1

        await writer.flush()
        assert len(writer._detections) == 0
        mock_db.add_all.assert_called_once()
        mock_db.commit.assert_called_once()


# ══════════════════════════════════════════════
# Pipeline manager tests
# ══════════════════════════════════════════════

class TestPipelineManager:
    @pytest.mark.asyncio
    async def test_submit_live_frame_caches_and_publishes(self, mock_redis, mock_producer):
        from app.Pipeline import PipelineManager
        pipeline = PipelineManager(redis_conn=mock_redis)
        pipeline.producer = mock_producer

        frame_id = await pipeline.submit_live_frame(
            session_id="session-123",
            frame_data=b"fake_frame_bytes",
            frame_number=0,
            timestamp_ms=0.0,
        )

        assert frame_id is not None
        mock_redis.set.assert_called_once()
        mock_producer.send_and_wait.assert_called_once()

    @pytest.mark.asyncio
    async def test_submit_upload_job_publishes(self, mock_redis, mock_producer):
        from app.Pipeline import PipelineManager
        pipeline = PipelineManager(redis_conn=mock_redis)
        pipeline.producer = mock_producer

        await pipeline.submit_upload_job(
            session_id="session-456",
            mode=SessionMode.VIDEO,
            s3_key="uploads/session-456/video.mp4",
            priority=3,
        )

        mock_producer.send_and_wait.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_media_result_no_faces_skips(self, mock_redis, mock_producer, fake_session):
        from app.Pipeline import PipelineManager
        pipeline = PipelineManager(redis_conn=mock_redis)
        pipeline.producer = mock_producer

        payload = MediaResult(
            task_id="t1", session_id=str(fake_session.id), frame_id="f1",
            frame_number=0, timestamp_ms=0.0,
            faces=[], processing_time_ms=5.0, worker_id="w1",
        ).model_dump()

        with patch("app.Pipeline.async_session") as mock_sf:
            mock_db = AsyncMock()
            mock_sf.return_value.__aenter__ = AsyncMock(return_value=mock_db)
            mock_sf.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_db.execute = AsyncMock(return_value=MagicMock(scalar_one_or_none=MagicMock(return_value=fake_session)))

            await pipeline._handle_media_result(payload, priority=1)

        mock_producer.send_and_wait.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_inference_result_publishes_to_redis(self, mock_redis, mock_producer, sample_inference_result):
        from app.Pipeline import PipelineManager
        pipeline = PipelineManager(redis_conn=mock_redis)
        pipeline.producer = mock_producer

        await pipeline._handle_inference_result(sample_inference_result.model_dump(), priority=1)

        mock_redis.xadd.assert_called_once()
        mock_redis.hset.assert_called()

    @pytest.mark.asyncio
    async def test_handle_inference_result_tracks_predictions(self, mock_redis, mock_producer, sample_inference_result):
        from app.Pipeline import PipelineManager
        pipeline = PipelineManager(redis_conn=mock_redis)
        pipeline.producer = mock_producer

        session_id = sample_inference_result.session_id
        await pipeline._handle_inference_result(sample_inference_result.model_dump(), priority=1)

        assert session_id in pipeline._session_predictions
        assert len(pipeline._session_predictions[session_id]) == 1
        assert pipeline._session_predictions[session_id][0]["faces"][0]["top_emotion"] == "happy"

    @pytest.mark.asyncio
    async def test_queue_burn_job_publishes(self, mock_redis, mock_producer):
        from app.Pipeline import PipelineManager
        pipeline = PipelineManager(redis_conn=mock_redis)
        pipeline.producer = mock_producer

        session_id = str(uuid.uuid4())
        pipeline._session_predictions[session_id] = [{
            "frame_number": 0,
            "timestamp_ms": 0.0,
            "faces": [{"bbox": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "top_emotion": "happy", "top_confidence": 0.9}],
        }]

        with patch("app.Pipeline.async_session") as mock_sf:
            mock_db = AsyncMock()
            mock_sf.return_value.__aenter__ = AsyncMock(return_value=mock_db)
            mock_sf.return_value.__aexit__ = AsyncMock(return_value=False)

            await pipeline.queue_burn_job(
                session_id=session_id,
                burn_type=BurnType.LIVE_EXPORT,
                source_type=MediaSourceType.FRAME_SEQUENCE,
            )

        mock_producer.send_and_wait.assert_called_once()
        mock_redis.hset.assert_called()


# ══════════════════════════════════════════════
# API route tests (real main.py, mocked infrastructure)
# ══════════════════════════════════════════════

class TestAPIRoutes:
    def test_health_endpoint(self, test_client):
        response = test_client.get("/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "orchestrator"
        assert data["status"] == "ok"

    def test_create_session_endpoint(self, test_client, fake_session):
        response = test_client.post("/api/sessions", json={"mode": "live"})
        assert response.status_code == 200
        data = response.json()
        assert data["session_id"] == str(fake_session.id)
        assert data["mode"] == "live"
        assert data["status"] == "active"

    def test_create_video_session_endpoint(self, mock_redis, mock_producer):
        from fastapi.testclient import TestClient

        fake = MagicMock()
        fake.id = uuid.uuid4()
        fake.mode = "video"
        fake.status = "processing"
        fake.created_at = datetime.now(timezone.utc)

        pipeline = AsyncMock()
        pipeline.create_session = AsyncMock(return_value=fake)
        pipeline.redis = mock_redis
        pipeline.start = AsyncMock()
        pipeline.stop = AsyncMock()

        with patch("app.main.ensure_topics", new_callable=AsyncMock), \
             patch("app.main.get_redis", new_callable=AsyncMock, return_value=mock_redis), \
             patch("app.main.PipelineManager", return_value=pipeline):
            from main import app
            with TestClient(app) as c:
                response = c.post("/api/sessions", json={"mode": "video", "source_s3_key": "uploads/test/video.mp4"})
                assert response.status_code == 200
                assert response.json()["mode"] == "video"
                assert response.json()["status"] == "processing"

    def test_submit_frame_endpoint(self, test_client):
        frame_b64 = base64.b64encode(b"fake_frame").decode()
        response = test_client.post("/api/frames", json={
            "session_id": "session-123",
            "frame_data": frame_b64,
            "frame_number": 0,
        })
        assert response.status_code == 200
        assert response.json()["frame_id"] == "frame-id-123"

    def test_submit_frame_invalid_base64(self, test_client):
        response = test_client.post("/api/frames", json={
            "session_id": "session-123",
            "frame_data": "not_valid_base64!!!",
            "frame_number": 0,
        })
        assert response.status_code == 400

    def test_session_status_from_redis(self, test_client, mock_redis):
        mock_redis.hgetall = AsyncMock(return_value={
            b"status": b"processing",
            b"progress": b"0.5",
            b"total_frames": b"100",
            b"current_frame": b"50",
        })
        response = test_client.get("/api/sessions/some-session-id/status")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "processing"
        assert data["progress"] == 0.5
        assert data["current_frame"] == 50

    def test_session_not_found(self, test_client, mock_redis, mock_pipeline):
        mock_pipeline.get_session = AsyncMock(return_value=None)
        mock_redis.hgetall = AsyncMock(return_value={})
        response = test_client.get("/api/sessions/nonexistent/status")
        assert response.status_code == 404

    def test_list_sessions_endpoint(self, test_client):
        response = test_client.get("/api/sessions")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["mode"] == "live"


# ══════════════════════════════════════════════
# Redis utils tests
# ══════════════════════════════════════════════

class TestRedisUtils:
    @pytest.mark.asyncio
    async def test_cache_frame(self, mock_redis):
        from app.Redis import cache_frame
        await cache_frame(mock_redis, "s1", "f1", b"frame_bytes")
        mock_redis.set.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_live(self, mock_redis):
        from app.Redis import publish_live
        await publish_live(mock_redis, "session-1", {"test": "data"})
        mock_redis.xadd.assert_called_once()

    @pytest.mark.asyncio
    async def test_set_job_status(self, mock_redis):
        from app.Redis import set_job_status
        await set_job_status(mock_redis, "s1", status="processing", progress=0.5)
        mock_redis.hset.assert_called_once()
        mock_redis.expire.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_job_status_empty(self, mock_redis):
        from app.Redis import get_job_status
        mock_redis.hgetall = AsyncMock(return_value={})
        result = await get_job_status(mock_redis, "s1")
        assert result is None

    @pytest.mark.asyncio
    async def test_session_active(self, mock_redis):
        from app.Redis import is_session_active
        mock_redis.exists = AsyncMock(return_value=1)
        assert await is_session_active(mock_redis, "s1") is True

    @pytest.mark.asyncio
    async def test_session_inactive(self, mock_redis):
        from app.Redis import is_session_active
        mock_redis.exists = AsyncMock(return_value=0)
        assert await is_session_active(mock_redis, "s1") is False

    @pytest.mark.asyncio
    async def test_end_session_cleans_up(self, mock_redis):
        from app.Redis import end_session
        await end_session(mock_redis, "s1")
        assert mock_redis.delete.call_count == 2


# ══════════════════════════════════════════════
# Kafka utils tests
# ══════════════════════════════════════════════

class TestKafkaUtils:
    def test_extract_priority_present(self):
        from app.Kafka import extract_priority
        message = MagicMock()
        message.headers = [("priority", b"2")]
        assert extract_priority(message) == 2

    def test_extract_priority_missing(self):
        from app.Kafka import extract_priority
        message = MagicMock()
        message.headers = None
        assert extract_priority(message) is None

    def test_extract_priority_no_priority_header(self):
        from app.Kafka import extract_priority
        message = MagicMock()
        message.headers = [("other", b"value")]
        assert extract_priority(message) is None
