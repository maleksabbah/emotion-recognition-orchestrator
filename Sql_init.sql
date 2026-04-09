-- ============================================================
-- PostgreSQL init — run at Docker startup
-- orchestrator_db: all business data
-- gateway_db: request logs + rate limits
-- Media service is stateless (no database)
-- ============================================================

CREATE DATABASE orchestrator_db;
CREATE DATABASE gateway_db;

-- ============================================================
-- ORCHESTRATOR DB
-- ============================================================
\c orchestrator_db;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TYPE session_mode AS ENUM ('live', 'video', 'photo');
CREATE TYPE session_status AS ENUM ('active', 'processing', 'burning', 'complete', 'failed');
CREATE TYPE landmark_tier AS ENUM ('mediapipe', 'insightface', 'fallback');
CREATE TYPE worker_type AS ENUM ('media', 'inference', 'burner');
CREATE TYPE task_type AS ENUM ('frame', 'video', 'photo', 'burn');
CREATE TYPE file_type AS ENUM ('upload', 'burned_photo', 'burned_video');

CREATE TABLE sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    mode session_mode NOT NULL,
    status session_status NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    total_frames INTEGER NOT NULL DEFAULT 0,
    total_faces INTEGER NOT NULL DEFAULT 0,
    source_s3_key TEXT,
    burned_s3_key TEXT,
    metadata JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX idx_sessions_status ON sessions(status);
CREATE INDEX idx_sessions_mode ON sessions(mode);
CREATE INDEX idx_sessions_created_at ON sessions(created_at DESC);

CREATE TABLE detections (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    frame_number INTEGER NOT NULL,
    timestamp_ms DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    face_index INTEGER NOT NULL DEFAULT 0,
    bbox_x DOUBLE PRECISION NOT NULL,
    bbox_y DOUBLE PRECISION NOT NULL,
    bbox_w DOUBLE PRECISION NOT NULL,
    bbox_h DOUBLE PRECISION NOT NULL,
    landmark_tier landmark_tier NOT NULL,
    track_id INTEGER NOT NULL DEFAULT 0,
    crops_s3_prefix TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_detections_session_id ON detections(session_id);
CREATE INDEX idx_detections_session_frame ON detections(session_id, frame_number);
CREATE INDEX idx_detections_track_id ON detections(session_id, track_id);

CREATE TABLE predictions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    detection_id UUID NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
    emotion_angry DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    emotion_disgust DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    emotion_fear DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    emotion_happy DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    emotion_neutral DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    emotion_sad DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    emotion_surprise DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    top_emotion VARCHAR(16) NOT NULL,
    top_confidence DOUBLE PRECISION NOT NULL,
    valence VARCHAR(16) NOT NULL,
    arousal VARCHAR(8) NOT NULL,
    intensity VARCHAR(8) NOT NULL,
    inference_time_ms DOUBLE PRECISION,
    worker_id VARCHAR(64)
);
CREATE INDEX idx_predictions_detection_id ON predictions(detection_id);
CREATE INDEX idx_predictions_top_emotion ON predictions(top_emotion);
CREATE INDEX idx_predictions_low_confidence ON predictions(top_confidence) WHERE top_confidence < 0.5;

CREATE TABLE media_files (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    file_type file_type NOT NULL,
    s3_key TEXT NOT NULL,
    content_type VARCHAR(64) NOT NULL,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    original_filename TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_media_files_session_id ON media_files(session_id);
CREATE INDEX idx_media_files_s3_key ON media_files(s3_key);

CREATE TABLE crop_records (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    detection_id UUID NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
    frame_number INTEGER NOT NULL,
    s3_prefix TEXT NOT NULL,
    face_key TEXT NOT NULL,
    eyes_key TEXT NOT NULL,
    mouth_key TEXT NOT NULL,
    cheeks_key TEXT NOT NULL,
    forehead_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_crop_records_session_id ON crop_records(session_id);
CREATE INDEX idx_crop_records_detection_id ON crop_records(detection_id);

CREATE TABLE worker_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID REFERENCES sessions(id) ON DELETE SET NULL,
    worker_id VARCHAR(64) NOT NULL,
    worker_type worker_type NOT NULL,
    task_type task_type NOT NULL,
    frames_processed INTEGER NOT NULL DEFAULT 0,
    processing_time_ms DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    device VARCHAR(32),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_worker_logs_session_id ON worker_logs(session_id);
CREATE INDEX idx_worker_logs_worker_id ON worker_logs(worker_id);
CREATE INDEX idx_worker_logs_created_at ON worker_logs(created_at DESC);

-- ============================================================
-- GATEWAY DB
-- ============================================================
\c gateway_db;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE request_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    method VARCHAR(10) NOT NULL,
    path TEXT NOT NULL,
    client_ip VARCHAR(45),
    status_code INTEGER,
    response_time_ms DOUBLE PRECISION,
    user_agent TEXT,
    session_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_request_logs_created_at ON request_logs(created_at DESC);
CREATE INDEX idx_request_logs_client_ip ON request_logs(client_ip);

CREATE TABLE rate_limits (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_ip VARCHAR(45) NOT NULL,
    endpoint VARCHAR(128) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    request_count INTEGER NOT NULL DEFAULT 1,
    UNIQUE(client_ip, endpoint, window_start)
);
CREATE INDEX idx_rate_limits_lookup ON rate_limits(client_ip, endpoint, window_start);