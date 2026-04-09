# EmotionRecognitionOrchestrator
# FastAPI: pipeline brain, Kafka pub/sub, Redis streams, PostgreSQL
# Talks to: Kafka, Redis, PostgreSQL (orchestrator_db), Storage Service (HTTP)
# Port: 8001

FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY main.py .

EXPOSE 8001

# Single worker — orchestrator manages stateful pipeline
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "1"]
