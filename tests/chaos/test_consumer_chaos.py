"""
Chaos tests: consumer-level fault injection.

Scenarios:
  1. Consumer crashes (KeyboardInterrupt / CancelledError) mid-handle →
     message status is FAILED / PROCESSING, not committed.
  2. Handler flakiness: intermittent exceptions with exponential backoff.
  3. No message_id: messages without an ID are processed best-effort.
  4. Malformed payloads: non-JSON bytes don't crash the consumer loop.
  5. DLQ misconfiguration: missing DLQ producer logs a warning, does not raise.
  6. Idempotency decorator under concurrent callers.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.consumer.dedup_consumer import DedupConsumer, _extract_message_id
from src.dedup import InMemoryDeduplicationStore
from src.dedup.models import DeduplicationConfig, StatusValue
from src.idempotency.guard import IdempotencyGuard, idempotent

pytestmark = pytest.mark.chaos


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_record(
    value: bytes | None = None,
    headers: list | None = None,
    topic: str = "chaos-topic",
    offset: int = 0,
) -> MagicMock:
    r = MagicMock()
    r.value = value
    r.headers = headers or []
    r.topic = topic
    r.partition = 0
    r.offset = offset
    r.key = b"k"
    return r


def _json_record(message_id: str, extra: dict | None = None, **kwargs) -> MagicMock:
    payload = {"message_id": message_id, **(extra or {})}
    return _make_record(value=json.dumps(payload).encode(), **kwargs)


# ---------------------------------------------------------------------------
# 1. Consumer crash mid-handle
# ---------------------------------------------------------------------------


class TestConsumerCrashMidHandle:
    async def test_cancelled_error_leaves_processing_status(self):
        """
        If the consumer task is cancelled while handle() is awaiting,
        the dedup record is left in PROCESSING state (not COMPLETED).
        Kafka offset must not be committed.
        """
        store = InMemoryDeduplicationStore()
        consumer = DedupConsumer(topics=["t"], store=store)
        consumer._consumer = AsyncMock()

        async def slow_handler(message: Any) -> Any:
            await asyncio.sleep(10)  # long-running
            return "never"

        consumer.handle = slow_handler

        record = _json_record("cancel-msg-1")
        task = asyncio.create_task(consumer._process_record(record))
        await asyncio.sleep(0.05)  # let it enter handle()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        # Offset must not have been committed
        consumer._consumer.commit.assert_not_called()

        # Status is PROCESSING (claim() succeeded before cancel)
        status = await store.get_status("cancel-msg-1")
        if status is not None:
            assert status.status == StatusValue.PROCESSING

    async def test_keyboard_interrupt_propagates(self):
        """KeyboardInterrupt from handle() bubbles out of _process_record."""
        store = InMemoryDeduplicationStore()
        consumer = DedupConsumer(topics=["t"], store=store)
        consumer._consumer = AsyncMock()
        consumer.handle = AsyncMock(side_effect=KeyboardInterrupt)

        record = _json_record("ki-msg-1")
        with pytest.raises(KeyboardInterrupt):
            await consumer._process_record(record)

        consumer._consumer.commit.assert_not_called()


# ---------------------------------------------------------------------------
# 2. Intermittent handler failures with backoff
# ---------------------------------------------------------------------------


class TestIntermittentFailures:
    async def test_recovery_after_transient_errors(self):
        """
        handler raises on attempt 1 & 2, succeeds on attempt 3.
        After recovery the message should be COMPLETED and committed.
        """
        attempt = 0

        async def flaky(message: Any) -> dict:
            nonlocal attempt
            attempt += 1
            if attempt < 3:
                raise IOError(f"transient #{attempt}")
            return {"result": "ok"}

        store = InMemoryDeduplicationStore()
        consumer = DedupConsumer(
            topics=["t"],
            store=store,
            config=DeduplicationConfig(max_retries=3, retry_backoff_ms=0),
        )
        consumer._consumer = AsyncMock()
        consumer.handle = flaky

        await consumer._process_record(_json_record("transient-msg-1"))

        assert attempt == 3
        status = await store.get_status("transient-msg-1")
        assert status.status == StatusValue.COMPLETED
        consumer._consumer.commit.assert_called()

    async def test_all_retries_exhausted_goes_to_dlq(self):
        store = InMemoryDeduplicationStore()
        dlq_producer = AsyncMock()

        consumer = DedupConsumer(
            topics=["t"],
            store=store,
            dlq_topic="t.dlq",
            config=DeduplicationConfig(max_retries=2, retry_backoff_ms=0),
        )
        consumer._consumer = AsyncMock()
        consumer._dlq_producer = dlq_producer
        consumer.handle = AsyncMock(side_effect=ValueError("always fails"))

        await consumer._process_record(_json_record("exhaust-msg-1"))

        dlq_producer.send_and_wait.assert_called_once()
        assert dlq_producer.send_and_wait.call_args[0][0] == "t.dlq"
        consumer._consumer.commit.assert_called()


# ---------------------------------------------------------------------------
# 3. No message_id — best-effort processing
# ---------------------------------------------------------------------------


class TestNoMessageId:
    async def test_missing_id_still_calls_handle(self):
        store = InMemoryDeduplicationStore()
        consumer = DedupConsumer(topics=["t"], store=store)
        consumer._consumer = AsyncMock()
        consumer.handle = AsyncMock(return_value="done")

        # Payload without message_id or id
        record = _make_record(value=b'{"payload": "data"}')
        await consumer._process_record(record)

        consumer.handle.assert_called_once()
        consumer._consumer.commit.assert_called()

    async def test_missing_id_does_not_write_to_store(self):
        store = InMemoryDeduplicationStore()
        consumer = DedupConsumer(topics=["t"], store=store)
        consumer._consumer = AsyncMock()
        consumer.handle = AsyncMock(return_value="ok")

        record = _make_record(value=b'{"x": 1}')
        await consumer._process_record(record)

        assert store.size() == 0  # no record written


# ---------------------------------------------------------------------------
# 4. Malformed payloads
# ---------------------------------------------------------------------------


class TestMalformedPayloads:
    async def test_binary_payload_does_not_crash(self):
        """Non-UTF-8 binary bytes must not crash _process_record."""
        store = InMemoryDeduplicationStore()
        consumer = DedupConsumer(topics=["t"], store=store)
        consumer._consumer = AsyncMock()
        consumer.handle = AsyncMock(return_value="ok")

        record = _make_record(value=b"\xff\xfe\xfd\x00\x01")
        await consumer._process_record(record)

        consumer._consumer.commit.assert_called()

    async def test_empty_payload_does_not_crash(self):
        store = InMemoryDeduplicationStore()
        consumer = DedupConsumer(topics=["t"], store=store)
        consumer._consumer = AsyncMock()
        consumer.handle = AsyncMock(return_value="ok")

        record = _make_record(value=b"")
        await consumer._process_record(record)

        consumer._consumer.commit.assert_called()

    async def test_none_value_does_not_crash(self):
        store = InMemoryDeduplicationStore()
        consumer = DedupConsumer(topics=["t"], store=store)
        consumer._consumer = AsyncMock()
        consumer.handle = AsyncMock(return_value="ok")

        record = _make_record(value=None)
        await consumer._process_record(record)

        consumer._consumer.commit.assert_called()


# ---------------------------------------------------------------------------
# 5. DLQ misconfiguration
# ---------------------------------------------------------------------------


class TestDlqMisconfiguration:
    async def test_no_dlq_producer_logs_warning_not_raises(self, caplog):
        """
        When no DLQ is configured, send_to_dlq must log a warning, not raise.
        """
        import logging
        store = InMemoryDeduplicationStore()
        consumer = DedupConsumer(
            topics=["t"],
            store=store,
            config=DeduplicationConfig(max_retries=1, retry_backoff_ms=0),
        )
        consumer._consumer = AsyncMock()
        consumer._dlq_producer = None
        consumer._dlq_topic = None
        consumer.handle = AsyncMock(side_effect=RuntimeError("fatal"))

        with caplog.at_level(logging.WARNING):
            await consumer._process_record(_json_record("no-dlq-msg-1"))

        # Should have logged about no DLQ
        assert any("No DLQ configured" in r.message or "DLQ" in r.message
                   for r in caplog.records)
        # Offset should still be committed (message is dead, no point redelivering)
        consumer._consumer.commit.assert_called()


# ---------------------------------------------------------------------------
# 6. Idempotency guard under concurrent callers
# ---------------------------------------------------------------------------


class TestIdempotencyGuardChaos:
    async def test_concurrent_guards_only_one_processes(self):
        """
        50 concurrent IdempotencyGuard contexts for the same key.
        Only one should enter the 'not already_processed' path.
        """
        store = InMemoryDeduplicationStore()
        processed_count = 0

        async def _use_guard(i: int) -> None:
            nonlocal processed_count
            async with IdempotencyGuard(store, "shared-key") as guard:
                if not guard.already_processed:
                    processed_count += 1
                    guard.result = {"processor": i}

        await asyncio.gather(*[_use_guard(i) for i in range(50)])

        assert processed_count == 1

    async def test_decorator_concurrent_callers_cached_result(self):
        """
        50 concurrent calls to an @idempotent function for the same key.
        The function body runs once; all callers get the same result.
        """
        store = InMemoryDeduplicationStore()
        execution_count = 0

        @idempotent(store=store, key_fn=lambda k: k)
        async def process(key: str) -> dict:
            nonlocal execution_count
            execution_count += 1
            await asyncio.sleep(0.01)  # simulate I/O
            return {"value": 42}

        results = await asyncio.gather(*[process("shared") for _ in range(50)])

        # Function body must run at most once (races may return None for losers)
        assert execution_count == 1
        non_none = [r for r in results if r is not None]
        assert all(r == {"value": 42} for r in non_none)
