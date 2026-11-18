"""
Kafka producer + consumer factories for orchestrator (batch flow).

main.py starts the producer in lifespan and stores it on app.state.
PipelineService consumes results via dedicated KafkaConsumer loops.
"""
from __future__ import annotations

import ssl
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.Config.Config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_SASL_USERNAME,
    KAFKA_SASL_PASSWORD,
    ORCHESTRATOR_GROUP_ID,
)


def _sasl_kwargs() -> dict:
    """Return SASL connection kwargs when credentials are configured."""
    if KAFKA_SASL_USERNAME:
        return {
            "security_protocol": "SASL_SSL",
            "ssl_context": ssl.create_default_context(),
            "sasl_mechanism": "SCRAM-SHA-256",
            "sasl_plain_username": KAFKA_SASL_USERNAME,
            "sasl_plain_password": KAFKA_SASL_PASSWORD,
        }
    return {}


def get_producer() -> AIOKafkaProducer:
    return AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        **_sasl_kwargs(),
    )


def make_consumer(topic: str) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=ORCHESTRATOR_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        session_timeout_ms=300000,
        max_poll_interval_ms=1800000,
        **_sasl_kwargs(),
    )