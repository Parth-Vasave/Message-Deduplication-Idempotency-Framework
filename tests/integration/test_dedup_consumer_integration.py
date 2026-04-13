"""
Integration tests for DedupConsumer against a real Kafka + Redis container.

These tests verify the end-to-end deduplication lifecycle:
  - Duplicate messages are skipped exactly once
  - Failed messages are retried and DLQ'd
  - Consumer starts, processes, and stops cleanly
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any

import pytest
from aiokafka import AIOKafkaProducer

from src.consumer.dedup_consumer import DedupConsumer
from src.dedup.models import DeduplicationConfig

pytestmark = pytest.mark.integration

_TOPIC = "test-dedup-consumer"
_DLQ_TOPIC = "test-dedup-consumer.dlq"


# ---------------------------------------------------------------------------
# Helper: produce a message with a given message_id
# ---------------------------------------------------------------------------


async def _produce(bootstrap: str, topic: str, message_id: str, payload: dict) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        body = json.dumps({"message_id": message_id, **payload}).encode()
        await producer.send_and_wait(topic, value=body)
    finally:
        await producer.stop()


# ---------------------------------------------------------------------------
# Helper: run consumer for a limited time, collecting processed message_ids
# ---------------------------------------------------------------------------


async def _run_consumer(
    consumer: DedupConsumer,
    timeout: float = 5.0,
) -> None:
    """Start consumer, let it run for `timeout` seconds, then stop it."""
    await consumer.start()
    try:
        run_task = asyncio.create_task(consumer.run())
        await asyncio.sleep(timeout)
        consumer._running = False
        run_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await run_task
    finally:
        await consumer.stop()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDedupConsumerIntegration:
    async def test_unique_messages_all_processed(self, kafka_bootstrap, redis_store):
        """5 unique messages → handler called exactly 5 times."""
        processed: list[str] = []

        class _TrackingConsumer(DedupConsumer):
            async def handle(self, message: Any) -> Any:
                processed.append(message["message_id"])
                return {"ok": True}

        consumer = _TrackingConsumer(
            topics=[_TOPIC],
            store=redis_store,
            bootstrap_servers=kafka_bootstrap,
            group_id="integration-test-unique",
        )

        # Produce 5 unique messages before starting the consumer
        for i in range(5):
            await _produce(kafka_bootstrap, _TOPIC, f"unique-{i}", {"seq": i})

        await asyncio.sleep(0.5)  # let Kafka settle
        await _run_consumer(consumer, timeout=6.0)

        assert len(processed) == 5
        assert sorted(processed) == [f"unique-{i}" for i in range(5)]

    async def test_duplicate_messages_processed_once(self, kafka_bootstrap, redis_store):
        """
        The same message_id sent 3 times → handler invoked only once.
        """
        processed: list[str] = []

        class _TrackingConsumer(DedupConsumer):
            async def handle(self, message: Any) -> Any:
                processed.append(message["message_id"])
                return {"done": True}

        consumer = _TrackingConsumer(
            topics=[_TOPIC],
            store=redis_store,
            bootstrap_servers=kafka_bootstrap,
            group_id="integration-test-dup",
        )

        for _ in range(3):
            await _produce(kafka_bootstrap, _TOPIC, "dup-msg-999", {"data": "x"})

        await asyncio.sleep(0.5)
        await _run_consumer(consumer, timeout=6.0)

        assert processed.count("dup-msg-999") == 1

    async def test_failed_handler_routes_to_dlq(self, kafka_bootstrap, redis_store):
        """
        A handler that always raises should exhaust retries and send to DLQ.
        """
        dlq_received: list[bytes] = []

        class _FailingConsumer(DedupConsumer):
            async def handle(self, message: Any) -> Any:
                raise RuntimeError("intentional failure")

        consumer = _FailingConsumer(
            topics=[_TOPIC],
            store=redis_store,
            bootstrap_servers=kafka_bootstrap,
            group_id="integration-test-dlq",
            dlq_topic=_DLQ_TOPIC,
            config=DeduplicationConfig(max_retries=1, retry_backoff_ms=0),
        )

        await _produce(kafka_bootstrap, _TOPIC, "dlq-msg-1", {"sentinel": True})

        await asyncio.sleep(0.5)
        await _run_consumer(consumer, timeout=6.0)

        # Verify DLQ message arrived
        AIOKafkaProducer.__new__(AIOKafkaProducer)
        from aiokafka import AIOKafkaConsumer as _AIOConsumer

        dlq_c = _AIOConsumer(
            _DLQ_TOPIC,
            bootstrap_servers=kafka_bootstrap,
            group_id="integration-dlq-verifier",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await dlq_c.start()
        found = False
        try:
            async def _scan() -> bool:
                async for record in dlq_c:
                    payload = json.loads(record.value)
                    if "dlq-msg-1" in payload.get("original_value", ""):
                        return True
                return False

            found = await asyncio.wait_for(_scan(), timeout=10.0)
        except asyncio.TimeoutError:
            pass
        finally:
            await dlq_c.stop()

        assert found, "dlq-msg-1 not found in DLQ"

    async def test_consumer_survives_restart(self, kafka_bootstrap, redis_store):
        """
        Completed messages are not re-processed after a consumer restart.
        """
        processed: list[str] = []

        class _TrackingConsumer(DedupConsumer):
            async def handle(self, message: Any) -> Any:
                processed.append(message["message_id"])
                return {"ok": True}

        await _produce(kafka_bootstrap, _TOPIC, "persist-msg-1", {})
        await asyncio.sleep(0.3)

        # First run
        consumer1 = _TrackingConsumer(
            topics=[_TOPIC],
            store=redis_store,
            bootstrap_servers=kafka_bootstrap,
            group_id="integration-test-restart",
        )
        await _run_consumer(consumer1, timeout=4.0)
        len(processed)

        # Second run with same consumer group and same store
        consumer2 = _TrackingConsumer(
            topics=[_TOPIC],
            store=redis_store,
            bootstrap_servers=kafka_bootstrap,
            group_id="integration-test-restart",
        )
        await _run_consumer(consumer2, timeout=4.0)

        # persist-msg-1 should appear exactly once across both runs
        assert processed.count("persist-msg-1") == 1
