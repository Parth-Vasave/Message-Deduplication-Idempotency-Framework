"""Unit tests for OutboxWriter and OutboxRelay."""

import asyncio
import json
from datetime import timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from src.idempotency.outbox import OutboxEntry, OutboxRelay, OutboxWriter


# ---------------------------------------------------------------------------
# OutboxEntry
# ---------------------------------------------------------------------------


class TestOutboxEntry:
    def test_created_at_defaults_to_now(self):
        entry = OutboxEntry(topic="orders", message_id="msg-1", payload={"k": "v"})
        assert entry.created_at is not None
        assert entry.created_at.tzinfo is not None

    def test_fields_stored(self):
        entry = OutboxEntry(topic="payments", message_id="pay-99", payload={"amount": 500})
        assert entry.topic == "payments"
        assert entry.message_id == "pay-99"
        assert entry.payload == {"amount": 500}


# ---------------------------------------------------------------------------
# OutboxWriter
# ---------------------------------------------------------------------------


class TestOutboxWriter:
    async def test_write_calls_execute(self):
        conn = AsyncMock()
        writer = OutboxWriter()
        entry = OutboxEntry(topic="orders", message_id="msg-1", payload={"order_id": 1})

        await writer.write(conn, entry)

        conn.execute.assert_called_once()
        args = conn.execute.call_args[0]
        assert args[1] == "orders"          # topic
        assert args[2] == "msg-1"           # message_id
        assert json.loads(args[3]) == {"order_id": 1}  # payload JSON

    async def test_write_passes_created_at(self):
        conn = AsyncMock()
        writer = OutboxWriter()
        entry = OutboxEntry(topic="t", message_id="m", payload={})

        await writer.write(conn, entry)

        args = conn.execute.call_args[0]
        assert args[4] == entry.created_at


# ---------------------------------------------------------------------------
# OutboxRelay
# ---------------------------------------------------------------------------


def _make_pool(rows: list[dict]) -> Any:
    """Build a mock asyncpg pool that returns the given rows on fetch."""
    # Transaction context manager
    txn_ctx = MagicMock()
    txn_ctx.__aenter__ = AsyncMock(return_value=None)
    txn_ctx.__aexit__ = AsyncMock(return_value=False)

    conn = MagicMock()
    conn.fetch = AsyncMock(return_value=rows)
    conn.execute = AsyncMock()
    conn.transaction = MagicMock(return_value=txn_ctx)

    # Pool acquire context manager
    acquire_ctx = MagicMock()
    acquire_ctx.__aenter__ = AsyncMock(return_value=conn)
    acquire_ctx.__aexit__ = AsyncMock(return_value=False)

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acquire_ctx)
    return pool, conn


class TestOutboxRelay:
    async def test_publishes_pending_rows(self):
        rows = [
            {"id": 1, "topic": "orders", "message_id": "msg-1", "payload": '{"a":1}'},
        ]
        pool, conn = _make_pool(rows)
        producer = AsyncMock()

        relay = OutboxRelay(pool=pool, producer=producer, poll_interval_s=999)
        published = await relay._process_batch()

        assert published == 1
        producer.send_and_wait.assert_called_once_with(
            "orders",
            value=b'{"a":1}',
            key=b"msg-1",
        )
        # Should have updated the row to PUBLISHED
        conn.execute.assert_called()

    async def test_marks_failed_on_producer_error(self):
        rows = [
            {"id": 7, "topic": "payments", "message_id": "pay-7", "payload": '{}'},
        ]
        pool, conn = _make_pool(rows)
        producer = AsyncMock()
        producer.send_and_wait.side_effect = ConnectionError("broker down")

        relay = OutboxRelay(pool=pool, producer=producer)
        published = await relay._process_batch()

        assert published == 0
        # Should have called the FAILED update SQL
        failed_calls = [c for c in conn.execute.call_args_list if "FAILED" in str(c)]
        assert len(failed_calls) == 1

    async def test_empty_batch_returns_zero(self):
        pool, conn = _make_pool([])
        producer = AsyncMock()

        relay = OutboxRelay(pool=pool, producer=producer)
        published = await relay._process_batch()

        assert published == 0
        producer.send_and_wait.assert_not_called()

    async def test_stop_halts_loop(self):
        pool, conn = _make_pool([])
        producer = AsyncMock()

        relay = OutboxRelay(pool=pool, producer=producer, poll_interval_s=0.01)
        task = asyncio.create_task(relay.start())
        await asyncio.sleep(0.05)
        await relay.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert relay._running is False

    async def test_publishes_multiple_rows(self):
        rows = [
            {"id": 1, "topic": "t", "message_id": "m-1", "payload": '{"x":1}'},
            {"id": 2, "topic": "t", "message_id": "m-2", "payload": '{"x":2}'},
            {"id": 3, "topic": "t", "message_id": "m-3", "payload": '{"x":3}'},
        ]
        pool, conn = _make_pool(rows)
        producer = AsyncMock()

        relay = OutboxRelay(pool=pool, producer=producer)
        published = await relay._process_batch()

        assert published == 3
        assert producer.send_and_wait.call_count == 3
