"""
Orchestrator integration tests — single file.
Run from orchestrator/ root: `pytest Test.py -v`

Requires:
  pip install asgi-lifespan aiokafka
  pytest.ini with session-scoped loops (see storage)
"""
from __future__ import annotations

import asyncio
import json
import os
import uuid

import httpx
import pytest
import pytest_asyncio
from aiokafka import AIOKafkaProducer
from asgi_lifespan import LifespanManager
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer


# ══════════════════════════════════════════════
# Containers
# ══════════════════════════════════════════════

@pytest.fixture(scope="session", autouse=True)
def postgres():
    with PostgresContainer("postgres:15") as pg:
        os.environ["POSTGRES_HOST"] = pg.get_container_host_ip()
        os.environ["POSTGRES_PORT"] = str(pg.get_exposed_port(5432))
        os.environ["POSTGRES_USER"] = pg.username
        os.environ["POSTGRES_PASSWORD"] = pg.password
        os.environ["ORCHESTRATOR_DB"] = pg.dbname
        yield


@pytest.fixture(scope="session", autouse=True)
def redis_url():
    with RedisContainer() as rc:
        url = f"redis://{rc.get_container_host_ip()}:{rc.get_exposed_port(6379)}"
        os.environ["REDIS_URL"] = url
        yield url


@pytest.fixture(scope="session", autouse=True)
def kafka_bootstrap():
    with KafkaContainer() as kc:
        bs = kc.get_bootstrap_server()
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = bs
        yield bs


@pytest.fixture(scope="session", autouse=True)
def downstream_env():
    os.environ["STORAGE_SERVICE_URL"] = "http://fake-storage:8002"
    os.environ.setdefault("JWT_SECRET_KEY", "test-secret")
    yield


# ══════════════════════════════════════════════
# App + client (session-scoped)
# ══════════════════════════════════════════════

@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def app_client(postgres, redis_url, kafka_bootstrap, downstream_env):
    from app.main import app
    from app.Config.Database import engine
    from app.Entities import Base

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as c:
            yield c, engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture(loop_scope="session")
async def client(app_client):
    c, engine = app_client
    from app.Entities import Base
    async with engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(table.delete())
    yield c


# ══════════════════════════════════════════════
# Tests
# ══════════════════════════════════════════════

@pytest.mark.asyncio(loop_scope="session")
async def test_health(client):
    r = await client.get("/health")
    assert r.status_code == 200


@pytest.mark.asyncio(loop_scope="session")
async def test_create_and_get_session(client):
    r = await client.post("/api/sessions", json={"mode": "live", "metadata": {}})
    assert r.status_code == 200
    sid = r.json()["id"]

    r = await client.get(f"/api/sessions/{sid}/status")
    assert r.status_code == 200
    assert r.json()["status"] in ("pending", "processing", "active")


@pytest.mark.asyncio(loop_scope="session")
async def test_list_sessions(client):
    await client.post("/api/sessions", json={"mode": "live", "metadata": {}})
    await client.post("/api/sessions", json={"mode": "video", "metadata": {}})
    r = await client.get("/api/sessions")
    assert r.status_code == 200
    assert len(r.json()) >= 2


@pytest.mark.asyncio(loop_scope="session")
async def test_end_session(client):
    sid = (await client.post("/api/sessions", json={"mode": "live", "metadata": {}})).json()["id"]
    r = await client.post(f"/api/sessions/{sid}/end")
    assert r.status_code == 202


@pytest.mark.asyncio(loop_scope="session")
async def test_media_result_creates_detections(client, kafka_bootstrap):
    sid = (await client.post("/api/sessions", json={"mode": "video", "metadata": {}})).json()["id"]

    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await producer.start()
    try:
        msg = {
            "task_id": str(uuid.uuid4()),
            "session_id": sid,
            "frame_number": 0,
            "timestamp_ms": 0.0,
            "faces": [{
                "face_index": 0,
                "track_id": 0,
                "bbox": {"x": 10, "y": 10, "w": 50, "h": 50},
                "landmark_tier": "mediapipe",
                "face_crop": "FAKE",
                "region_crops": {"eyes": "FAKE", "mouth": "FAKE", "cheeks": "FAKE", "forehead": "FAKE"},
                "crop_s3_keys": {
                    "face": "s/face.jpg", "eyes": "s/eyes.jpg",
                    "mouth": "s/mouth.jpg", "cheeks": "s/cheeks.jpg",
                    "forehead": "s/forehead.jpg",
                },
            }],
            "worker_id": "test",
            "processing_time_ms": 12.3,
        }
        await producer.send_and_wait("media_results", json.dumps(msg).encode())
    finally:
        await producer.stop()

    # Wait for the background consumer to process the message
    for _ in range(20):
        r = await client.get(f"/api/sessions/{sid}/status")
        if r.json().get("total_frames", 0) >= 1:
            break
        await asyncio.sleep(0.5)

    r = await client.get(f"/api/sessions/{sid}/status")
    status = r.json()
    assert status["total_frames"] >= 1
    # Face count may be reported under various keys depending on schema
    face_count = (
        status.get("total_faces")
        or status.get("total_detections")
        or status.get("faces_detected")
        or 0
    )
    assert face_count >= 1, f"No face count in status: {status}"