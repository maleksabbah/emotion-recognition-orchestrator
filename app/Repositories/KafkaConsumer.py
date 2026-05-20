"""
KafkaConsumer — subscribes to result topics and yields decoded messages.

PipelineService runs three consumer loops (media, inference, burn). Each
loop calls `consume(topic)` to get an async iterator of parsed dicts.
"""
from __future__ import annotations

import json
import logging
from typing import AsyncIterator

from aiokafka import AIOKafkaConsumer

from app.Config import (
    BURN_RESULTS_TOPIC,
    INFERENCE_RESULTS_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    MEDIA_RESULTS_TOPIC,
    ORCHESTRATOR_GROUP_ID,
)

logger = logging.getLogger("orchestrator.kafka-consumer")


class KafkaConsumer:
    """
    Factory + iterator for orchestrator's result consumers.

    Usage:
        consumer = KafkaConsumer()
        async for msg in consumer.consume(KafkaConsumer.MEDIA_RESULTS):
            handle(msg)
    """

    MEDIA_RESULTS = MEDIA_RESULTS_TOPIC
    INFERENCE_RESULTS = INFERENCE_RESULTS_TOPIC
    BURN_RESULTS = BURN_RESULTS_TOPIC

    async def consume(self, topic: str) -> AsyncIterator[dict]:
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=ORCHESTRATOR_GROUP_ID,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        try:
            async for record in consumer:
                try:
                    payload = json.loads(record.value.decode("utf-8"))
                except json.JSONDecodeError as e:
                    logger.error("Bad JSON from %s: %s", topic, e)
                    continue
                yield payload
        finally:
            await consumer.stop()