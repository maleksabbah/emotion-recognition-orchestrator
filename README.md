# EmotionRecognitionOrchestrator

Internal coordinator for the Mntis facial emotion recognition platform.
Owns the session lifecycle and drives the pipeline by publishing tasks
to Kafka and consuming results. Hosts the live-mode WebSocket at
`/ws/live`. Owns the `orchestrator_db` (sessions, detections,
predictions, crop records).

Built with FastAPI + aiokafka. Not exposed publicly — only the gateway
and the nginx WebSocket route reach it.

## Architecture

The service follows a clean, layered architecture where each layer has
one responsibility and depends only on the layer beneath it:

* **Routes** — thin HTTP / WebSocket controllers. Internal REST for
  session create / status / download, plus the `/ws/live` route that
  handles WebSocket handshakes, auth (token query param), and the
  per-frame ingress loop. They translate between HTTP/WS and DTOs and
  call the service layer. No business logic lives here.
* **Services** — the business logic. `PipelineService` reacts to
  Kafka results (media → inference → burn) and decides the next event
  to publish, marking the session complete when all fans converge.
  `JobService` builds the initial `MediaTask` for batch uploads.
  `LiveSessionService` runs a live WebSocket session: it caches each
  binary frame in Redis, enqueues a `MediaTask` onto the per-session
  Redis list, and pumps results from the per-session Redis stream
  back over the socket. `AuthService` decodes JWTs issued by the
  gateway (no users table here — orchestrator only validates and
  reads `sub`).
* **Repositories** — data access. `JobRepository` /
  `DetectionRepository` / `PredictionRepository` / `CropRecordRepository`
  wrap the database. `RedisRepository` wraps live-session state and
  the result stream. `KafkaProducer` and the consumer loop in
  `main.py` wrap Kafka. `StorageClient` is the typed HTTP client
  used to write crop and burned-image metadata into the storage
  service. The service layer never touches SQL, Redis commands, or
  raw Kafka directly.
* **Entities** — `Session`, `Detection`, `Prediction`, `CropRecord`
  ORM models, plus the in-flight `Job` aggregate the pipeline
  service reads. The schema is managed with Alembic — migrations
  live under `alembic/versions/` and run on startup via the
  FastAPI `lifespan`.
* **Dtos** — the typed task and result schemas exchanged on Kafka
  (`MediaTask`, `MediaResult`, `InferenceTask`, `InferenceResult`,
  `BurnTask`, `BurnResult`), and the REST + WebSocket payload shapes.
  Keeping these as Pydantic models means workers validate every
  message at the boundary.
* **Config** — wiring: SQLAlchemy session, async Redis client, Kafka
  consumer + producer (six topics — three for tasks, three for
  results), httpx client, lifespan that creates the schema and
  starts the consumer loop.

This separation keeps the HTTP layer swappable, the business logic
testable in isolation, and the data layer free to change without
touching the rest.

Two unusual things worth calling out: the service runs a long-running
Kafka consumer alongside the HTTP server (started from the FastAPI
`lifespan`), so events keep flowing even when no HTTP request is
active; and the live-mode pipeline uses Redis (per-session list +
stream) rather than Kafka, because live frames are small, ephemeral,
and need per-session ordering rather than persistent broadcast.

Part of a multi-service system — see the [platform overview](../EmotionRecognitionDocker)
for the full architecture, pipeline flow, and the other services.
