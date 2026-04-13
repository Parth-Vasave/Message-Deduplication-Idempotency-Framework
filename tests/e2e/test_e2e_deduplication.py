"""
Full-stack E2E deduplication tests.

These tests spin up real Kafka, Redis, and Postgres containers and verify
the system's exactly-once guarantee end-to-end:

  Produce N messages (with duplicates)
    → DedupConsumer filters duplicates via Redis
      → Business logic writes to Postgres (UNIQUE constraint as second guard)
        → Exactly N unique rows in the database

Also verifies:
  - Prometheus counter increments correctly
  - DLQ messages are routed and readable
  - Outbox relay publishes to Kafka without double-publishing
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from aiokafka import AIOKafkaConsumer as _AIOConsumer
from aiokafka import AIOKafkaProducer

from src.consumer.dedup_consumer import DedupConsumer
from src.dedup.models import DeduplicationConfig, StatusValue
from src.idempotency.outbox import OutboxEntry, OutboxRelay, OutboxWriter
from src.metrics.dedup_metrics import DeduplicationMetrics

pytestmark = pytest.mark.e2e

_BASE_TOPIC = "e2e-orders"
_DLQ_TOPIC = "e2e-orders.dlq"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _produce_batch(
    bootstrap: str,
    topic: str,
    messages: list[dict],
) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        for msg in messages:
            body = json.dumps(msg).encode()
            await producer.send_and_wait(topic, value=body)
    finally:
        await producer.stop()


async def _run_consumer(consumer: DedupConsumer, timeout: float = 8.0) -> None:
    await consumer.start()
    try:
        task = asyncio.create_task(consumer.run())
        await asyncio.sleep(timeout)
        consumer._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    finally:
        await consumer.stop()


async def _count_orders(pool: Any) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT COUNT(*) AS n FROM orders")
        return row["n"]


async def _fetch_orders(pool: Any) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM orders ORDER BY id")
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# 1. Core exactly-once guarantee
# ---------------------------------------------------------------------------


class TestExactlyOnceGuarantee:
    async def test_5_unique_messages_create_5_orders(
        self, e2e_bootstrap, e2e_redis_store, e2e_pg_pool
    ):
        """
        Produce 5 unique messages → exactly 5 orders in Postgres.
        """
        pool = e2e_pg_pool

        class _OrderConsumer(DedupConsumer):
            async def handle(self, message: Any) -> dict:
                async with pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO orders (dedup_message_id, customer_id, amount) "
                        "VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                        message["message_id"],
                        message.get("customer_id", "cust-0"),
                        float(message.get("amount", 1.0)),
                    )
                return {"inserted": True}

        consumer = _OrderConsumer(
            topics=[_BASE_TOPIC],
            store=e2e_redis_store,
            bootstrap_servers=e2e_bootstrap,
            group_id="e2e-unique-orders",
        )

        messages = [
            {"message_id": f"e2e-order-{i}", "customer_id": f"cust-{i}", "amount": i * 10.0}
            for i in range(5)
        ]
        await _produce_batch(e2e_bootstrap, _BASE_TOPIC, messages)
        await asyncio.sleep(0.5)
        await _run_consumer(consumer, timeout=8.0)

        order_count = await _count_orders(pool)
        assert order_count == 5

    async def test_duplicates_not_inserted_twice(
        self, e2e_bootstrap, e2e_redis_store, e2e_pg_pool
    ):
        """
        Same message_id sent 5 times → exactly 1 order row.
        The dedup store (Redis) filters the first duplicate; the DB UNIQUE
        constraint acts as the second guard.
        """
        pool = e2e_pg_pool

        class _OrderConsumer(DedupConsumer):
            async def handle(self, message: Any) -> dict:
                async with pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO orders (dedup_message_id, customer_id, amount) "
                        "VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                        message["message_id"],
                        "cust-dup",
                        99.0,
                    )
                return {"inserted": True}

        consumer = _OrderConsumer(
            topics=[_BASE_TOPIC],
            store=e2e_redis_store,
            bootstrap_servers=e2e_bootstrap,
            group_id="e2e-dup-orders",
        )

        # Same message_id 5 times
        messages = [{"message_id": "e2e-dup-order-1", "customer_id": "cust-dup", "amount": 99.0}] * 5
        await _produce_batch(e2e_bootstrap, _BASE_TOPIC, messages)
        await asyncio.sleep(0.5)
        await _run_consumer(consumer, timeout=8.0)

        order_count = await _count_orders(pool)
        assert order_count == 1

    async def test_mixed_unique_and_duplicate_messages(
        self, e2e_bootstrap, e2e_redis_store, e2e_pg_pool
    ):
        """
        3 unique messages + 7 duplicate deliveries → exactly 3 order rows.
        """
        pool = e2e_pg_pool

        class _OrderConsumer(DedupConsumer):
            async def handle(self, message: Any) -> dict:
                async with pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO orders (dedup_message_id, customer_id, amount) "
                        "VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                        message["message_id"],
                        message.get("customer_id", "x"),
                        10.0,
                    )
                return {"ok": True}

        consumer = _OrderConsumer(
            topics=[_BASE_TOPIC],
            store=e2e_redis_store,
            bootstrap_servers=e2e_bootstrap,
            group_id="e2e-mixed-orders",
        )

        messages = []
        for i in range(3):
            # Each unique message appears 3-4 times
            for _ in range(3 if i < 2 else 4):
                messages.append({
                    "message_id": f"e2e-mixed-{i}",
                    "customer_id": f"cust-{i}",
                    "amount": float(i + 1) * 5,
                })

        await _produce_batch(e2e_bootstrap, _BASE_TOPIC, messages)
        await asyncio.sleep(0.5)
        await _run_consumer(consumer, timeout=8.0)

        order_count = await _count_orders(pool)
        assert order_count == 3


# ---------------------------------------------------------------------------
# 2. DLQ routing — poison-pill messages are isolated
# ---------------------------------------------------------------------------


class TestDlqRouting:
    async def test_poison_pill_goes_to_dlq_not_orders(
        self, e2e_bootstrap, e2e_redis_store, e2e_pg_pool
    ):
        """
        A message that always fails should end up in DLQ with no order created.
        """
        pool = e2e_pg_pool

        class _PoisonConsumer(DedupConsumer):
            async def handle(self, message: Any) -> Any:
                if message.get("poison"):
                    raise ValueError("poison pill!")
                return {"ok": True}

        consumer = _PoisonConsumer(
            topics=[_BASE_TOPIC],
            store=e2e_redis_store,
            bootstrap_servers=e2e_bootstrap,
            group_id="e2e-dlq-test",
            dlq_topic=_DLQ_TOPIC,
            config=DeduplicationConfig(max_retries=1, retry_backoff_ms=0),
        )

        messages = [{"message_id": "e2e-poison-1", "poison": True}]
        await _produce_batch(e2e_bootstrap, _BASE_TOPIC, messages)
        await asyncio.sleep(0.5)
        await _run_consumer(consumer, timeout=6.0)

        # No orders should have been created
        assert await _count_orders(pool) == 0

        # DLQ should have the message
        dlq_c = _AIOConsumer(
            _DLQ_TOPIC,
            bootstrap_servers=e2e_bootstrap,
            group_id="e2e-dlq-verifier",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        dlq_messages = []
        await dlq_c.start()
        try:
            async with asyncio.timeout(3.0):
                async for record in dlq_c:
                    dlq_messages.append(json.loads(record.value))
                    if dlq_messages:
                        break
        except (asyncio.TimeoutError, TimeoutError):
            pass
        finally:
            await dlq_c.stop()

        assert len(dlq_messages) >= 1
        assert any("e2e-poison-1" in str(m) for m in dlq_messages)


# ---------------------------------------------------------------------------
# 3. Outbox → Kafka → Consumer pipeline
# ---------------------------------------------------------------------------


class TestOutboxPipeline:
    async def test_outbox_relay_publishes_to_kafka(
        self, e2e_bootstrap, e2e_redis_store, e2e_pg_pool
    ):
        """
        Full pipeline:
          1. Write OutboxEntry to Postgres in a transaction
          2. OutboxRelay publishes to Kafka
          3. DedupConsumer processes from Kafka exactly once
        """
        pool = e2e_pg_pool
        _OUTBOX_TOPIC = "e2e-outbox-orders"
        processed: list[str] = []

        class _TrackingConsumer(DedupConsumer):
            async def handle(self, message: Any) -> Any:
                processed.append(message.get("message_id", "unknown"))
                return {"ok": True}

        # Step 1: write 3 entries to outbox
        writer = OutboxWriter()
        async with pool.acquire() as conn:
            async with conn.transaction():
                for i in range(3):
                    await writer.write(
                        conn,
                        OutboxEntry(
                            topic=_OUTBOX_TOPIC,
                            message_id=f"outbox-e2e-{i}",
                            payload={"message_id": f"outbox-e2e-{i}", "seq": i},
                        ),
                    )

        # Step 2: run relay to publish to Kafka
        kafka_producer = AIOKafkaProducer(bootstrap_servers=e2e_bootstrap)
        await kafka_producer.start()
        relay = OutboxRelay(pool=pool, producer=kafka_producer, poll_interval_s=999)
        try:
            published = await relay._process_batch()
        finally:
            await kafka_producer.stop()

        assert published == 3

        # Step 3: consume from Kafka
        consumer = _TrackingConsumer(
            topics=[_OUTBOX_TOPIC],
            store=e2e_redis_store,
            bootstrap_servers=e2e_bootstrap,
            group_id="e2e-outbox-consumer",
        )

        await asyncio.sleep(0.5)
        await _run_consumer(consumer, timeout=6.0)

        assert len(processed) == 3
        assert sorted(processed) == [f"outbox-e2e-{i}" for i in range(3)]


# ---------------------------------------------------------------------------
# 4. Metrics correctness
# ---------------------------------------------------------------------------


class TestMetrics:
    async def test_duplicate_counter_increments(
        self, e2e_bootstrap, e2e_redis_store
    ):
        """
        Produce 1 unique + 4 duplicates → dedup_messages_total{status='duplicate'}
        should increment by 4.
        """
        from prometheus_client import REGISTRY

        # Read baseline
        def _get_counter(status: str) -> float:
            try:
                return REGISTRY.get_sample_value(
                    "dedup_messages_total",
                    labels={"topic": _BASE_TOPIC, "status": status},
                ) or 0.0
            except Exception:
                return 0.0

        baseline_dup = _get_counter("duplicate")
        baseline_proc = _get_counter("processed")

        class _NoopConsumer(DedupConsumer):
            async def handle(self, message: Any) -> Any:
                return {"ok": True}

        consumer = _NoopConsumer(
            topics=[_BASE_TOPIC],
            store=e2e_redis_store,
            bootstrap_servers=e2e_bootstrap,
            group_id="e2e-metrics-test",
        )

        messages = [{"message_id": "e2e-metric-1"}] * 5  # 1 unique + 4 dups
        await _produce_batch(e2e_bootstrap, _BASE_TOPIC, messages)
        await asyncio.sleep(0.5)
        await _run_consumer(consumer, timeout=6.0)

        final_proc = _get_counter("processed")
        final_dup = _get_counter("duplicate")

        assert (final_proc - baseline_proc) >= 1    # at least 1 processed
        assert (final_dup - baseline_dup) >= 4      # at least 4 duplicates skipped
