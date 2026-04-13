"""
Integration tests for OutboxWriter and OutboxRelay against a real Postgres container.

Verifies:
  - OutboxWriter inserts rows correctly
  - OutboxRelay reads PENDING rows, publishes them, and marks PUBLISHED
  - FOR UPDATE SKIP LOCKED prevents double-publishing by concurrent relays
  - Producer failures mark rows FAILED without corrupting other rows
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any
from unittest.mock import AsyncMock

import pytest

from src.idempotency.outbox import OutboxEntry, OutboxRelay, OutboxWriter

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helper: count outbox rows by status
# ---------------------------------------------------------------------------


async def _count(pool: Any, status: str) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT COUNT(*) AS n FROM outbox WHERE status = $1", status)
        return row["n"]


async def _fetch_all(pool: Any) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM outbox ORDER BY id")
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# OutboxWriter integration
# ---------------------------------------------------------------------------


class TestOutboxWriterIntegration:
    async def test_write_inserts_pending_row(self, pg_pool):
        writer = OutboxWriter()
        entry = OutboxEntry(topic="orders", message_id="int-out-1", payload={"order_id": 1})

        async with pg_pool.acquire() as conn, conn.transaction():
            await writer.write(conn, entry)

        rows = await _fetch_all(pg_pool)
        assert len(rows) == 1
        assert rows[0]["topic"] == "orders"
        assert rows[0]["message_id"] == "int-out-1"
        assert rows[0]["status"] == "PENDING"
        assert json.loads(rows[0]["payload"]) == {"order_id": 1}

    async def test_write_on_conflict_does_nothing(self, pg_pool):
        """Writing the same message_id twice should silently ignore the second."""
        writer = OutboxWriter()
        entry = OutboxEntry(topic="orders", message_id="int-out-dup", payload={"v": 1})

        async with pg_pool.acquire() as conn, conn.transaction():
            await writer.write(conn, entry)
            await writer.write(conn, entry)  # duplicate

        rows = await _fetch_all(pg_pool)
        assert len(rows) == 1

    async def test_write_multiple_entries(self, pg_pool):
        writer = OutboxWriter()
        entries = [
            OutboxEntry(topic="t", message_id=f"int-bulk-{i}", payload={"i": i}) for i in range(5)
        ]
        async with pg_pool.acquire() as conn, conn.transaction():
            for e in entries:
                await writer.write(conn, e)

        rows = await _fetch_all(pg_pool)
        assert len(rows) == 5
        assert all(r["status"] == "PENDING" for r in rows)


# ---------------------------------------------------------------------------
# OutboxRelay integration
# ---------------------------------------------------------------------------


class TestOutboxRelayIntegration:
    async def test_relay_publishes_pending_and_marks_published(self, pg_pool):
        # Insert a PENDING outbox row directly
        async with pg_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO outbox (topic, message_id, payload) VALUES ($1, $2, $3)",
                "payments",
                "relay-msg-1",
                '{"amount": 100}',
            )

        producer = AsyncMock()
        relay = OutboxRelay(pool=pg_pool, producer=producer, batch_size=10)
        published = await relay._process_batch()

        assert published == 1
        producer.send_and_wait.assert_called_once_with(
            "payments",
            value=b'{"amount": 100}',
            key=b"relay-msg-1",
        )

        rows = await _fetch_all(pg_pool)
        assert rows[0]["status"] == "PUBLISHED"
        assert rows[0]["published_at"] is not None

    async def test_relay_marks_failed_on_producer_error(self, pg_pool):
        async with pg_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO outbox (topic, message_id, payload) VALUES ($1, $2, $3)",
                "orders",
                "relay-fail-1",
                '{"order_id": 99}',
            )

        producer = AsyncMock()
        producer.send_and_wait.side_effect = ConnectionError("broker unavailable")

        relay = OutboxRelay(pool=pg_pool, producer=producer)
        published = await relay._process_batch()

        assert published == 0
        rows = await _fetch_all(pg_pool)
        assert rows[0]["status"] == "FAILED"

    async def test_relay_skips_already_published(self, pg_pool):
        """Relay must only fetch PENDING rows; PUBLISHED rows are not reprocessed."""
        async with pg_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO outbox (topic, message_id, payload, status) VALUES ($1,$2,$3,$4)",
                "orders",
                "relay-published-1",
                "{}",
                "PUBLISHED",
            )

        producer = AsyncMock()
        relay = OutboxRelay(pool=pg_pool, producer=producer)
        published = await relay._process_batch()

        assert published == 0
        producer.send_and_wait.assert_not_called()

    async def test_relay_processes_batch_of_multiple(self, pg_pool):
        async with pg_pool.acquire() as conn:
            for i in range(5):
                await conn.execute(
                    "INSERT INTO outbox (topic, message_id, payload) VALUES ($1,$2,$3)",
                    "t",
                    f"batch-{i}",
                    f'{{"i":{i}}}',
                )

        producer = AsyncMock()
        relay = OutboxRelay(pool=pg_pool, producer=producer, batch_size=10)
        published = await relay._process_batch()

        assert published == 5
        assert producer.send_and_wait.call_count == 5
        assert await _count(pg_pool, "PUBLISHED") == 5

    async def test_concurrent_relays_no_double_publish(self, pg_pool):
        """
        Two concurrent relay instances run simultaneously.
        FOR UPDATE SKIP LOCKED ensures each row is published exactly once.
        """
        async with pg_pool.acquire() as conn:
            for i in range(10):
                await conn.execute(
                    "INSERT INTO outbox (topic, message_id, payload) VALUES ($1,$2,$3)",
                    "t",
                    f"concurrent-{i}",
                    "{}",
                )

        publish_calls: list[str] = []

        async def _counting_send(topic, *, value, key):
            publish_calls.append(key.decode())

        producer1 = AsyncMock()
        producer1.send_and_wait.side_effect = _counting_send
        producer2 = AsyncMock()
        producer2.send_and_wait.side_effect = _counting_send

        relay1 = OutboxRelay(pool=pg_pool, producer=producer1, batch_size=10)
        relay2 = OutboxRelay(pool=pg_pool, producer=producer2, batch_size=10)

        results = await asyncio.gather(
            relay1._process_batch(),
            relay2._process_batch(),
        )

        total_published = sum(results)
        assert total_published == 10  # all 10 rows published
        assert len(set(publish_calls)) == 10  # no duplicates in call keys
        assert len(publish_calls) == 10  # each published exactly once

    async def test_relay_stop_halts_background_loop(self, pg_pool):
        producer = AsyncMock()
        relay = OutboxRelay(pool=pg_pool, producer=producer, poll_interval_s=0.05)

        task = asyncio.create_task(relay.start())
        await asyncio.sleep(0.2)
        await relay.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert relay._running is False
