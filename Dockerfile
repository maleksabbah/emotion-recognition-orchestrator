# EmotionRecognitionOrchestrator
# FastAPI: pipeline brain, WebSocket terminator, Kafka pub/sub, Redis streams
# Talks to: Kafka, Redis, PostgreSQL (orchestrator_db), Storage (HTTP)
# Port: 8001

FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY main.py .

EXPOSE 8001

# Single worker — orchestrator owns stateful Kafka consumers + WebSocket sessions
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8001} --workers 1"]
