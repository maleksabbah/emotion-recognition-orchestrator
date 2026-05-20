"""
KafkaProducer — publishes orchestrator tasks to Kafka.

Wraps an aiokafka producer with three named send methods, one per task
topic. The PipelineService calls these; it never touches aiokafka.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from aiokafka import AIOKafkaProducer

from app.Config import (
    BURN_TASKS_TOPIC,
    INFERENCE_TASKS_TOPIC,
    MEDIA_TASKS_TOPIC,
)

logger = logging.getLogger("orchestrator.kafka-producer")


class KafkaProducer:
    def __init__(self, producer: AIOKafkaProducer):
        self.producer = producer

    async def publish_media_task(self, task: dict[str, Any]) -> None:
        await self._send(MEDIA_TASKS_TOPIC, task)

    async def publish_inference_task(self, task: dict[str, Any]) -> None:
        await self._send(INFERENCE_TASKS_TOPIC, task)

    async def publish_burn_task(self, task: dict[str, Any]) -> None:
        await self._send(BURN_TASKS_TOPIC, task)

    async def _send(self, topic: str, payload: dict[str, Any]) -> None:
        try:
            await self.producer.send_and_wait(topic, json.dumps(payload).encode("utf-8"))
        except Exception as e:
            logger.error("Kafka publish to %s failed: %s", topic, e)
            raise