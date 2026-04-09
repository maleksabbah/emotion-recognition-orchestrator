"""
Kafka utilities — producer/consumer wrappers for pipeline communication.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Callable, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from app.Config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_NUM_PARTITIONS,
    KAFKA_REPLICATION_FACTOR,
)

logger = logging.getLogger(__name__)


async def create_producer() -> AIOKafkaProducer:
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retry_backoff_ms=100,
        max_request_size=10_485_760,  # 10MB for base64 crops
    )
    await producer.start()
    logger.info("Kafka producer started")
    return producer


async def create_consumer(
    topic: str,
    group_id: str,
    auto_offset_reset: str = "earliest",
) -> AIOKafkaConsumer:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=False,
        max_poll_records=10,
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
        max_partition_fetch_bytes=10_485_760,
    )
    await consumer.start()
    logger.info(f"Kafka consumer started: topic={topic} group={group_id}")
    return consumer


async def publish(
    producer: AIOKafkaProducer,
    topic: str,
    payload: dict[str, Any],
    key: Optional[str] = None,
    priority: Optional[int] = None,
) -> None:
    headers = []
    if priority is not None:
        headers.append(("priority", str(priority).encode("utf-8")))
    await producer.send_and_wait(
        topic,
        value=payload,
        key=key,
        headers=headers or None,
    )


def extract_priority(message) -> Optional[int]:
    if message.headers:
        for k, v in message.headers:
            if k == "priority":
                return int(v.decode("utf-8"))
    return None


async def ensure_topics(topics: list[str]) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        existing = await admin.list_topics()
        new_topics = [
            NewTopic(
                name=t,
                num_partitions=KAFKA_NUM_PARTITIONS,
                replication_factor=KAFKA_REPLICATION_FACTOR,
            )
            for t in topics if t not in existing
        ]
        if new_topics:
            await admin.create_topics(new_topics)
            logger.info(f"Created topics: {[t.name for t in new_topics]}")
    finally:
        await admin.close()