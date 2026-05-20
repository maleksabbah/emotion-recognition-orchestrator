"""
Kafka producer + consumer factories for orchestrator (batch flow).

main.py starts the producer in lifespan and stores it on app.state.
PipelineService consumes results via dedicated KafkaConsumer loops.
"""
from __future__ import annotations

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.Config.Config import KAFKA_BOOTSTRAP_SERVERS, ORCHESTRATOR_GROUP_ID


def get_producer() -> AIOKafkaProducer:
    return AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)


def make_consumer(topic: str) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=ORCHESTRATOR_GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )