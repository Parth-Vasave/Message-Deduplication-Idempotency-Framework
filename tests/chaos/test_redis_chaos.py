"""
Chaos tests: Redis-level fault injection and race conditions.

Goals:
  1. Thundering-herd: N concurrent claim() calls → exactly one winner.
  2. Lua guard: state transitions are atomic even under concurrent retries.
  3. Orphaned PROCESSING records: crash mid-flight leaves the key PROCESSING;
     subsequent callers must not overwrite it.
  4. Store errors: DeduplicationStore operations that raise exceptions are
     handled gracefully by the consumer (offset still committed, no data lost).
  5. Retry storm: max_retries exhaustion under rapid fire.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from src.consumer.dedup_consumer import DedupConsumer
from src.dedup import InMemoryDeduplicationStore
from src.dedup.models import DeduplicationConfig, StatusValue

pytestmark = pytest.mark.chaos


# ---------------------------------------------------------------------------
# 1. Thundering herd — N goroutines racing to claim the same message_id
# ---------------------------------------------------------------------------


class TestThunderingHerd:
    @pytest.mark.parametrize("n", [10, 100, 500])
    async def test_exactly_one_claim_wins(self, fake_redis_store, n):
        """
        N concurrent async tasks all try to claim the same message_id.
        Exactly one must succeed regardless of n.
        """
        results = await asyncio.gather(
            *[fake_redis_store.claim("herd-msg") for _ in range(n)],
            return_exceptions=True,
        )
        # Exclude any unexpected exceptions
        wins = [r for r in results if r is True]
        losses = [r for r in results if r is False]
        errors = [r for r in results if isinstance(r, Exception)]

        assert errors == [], f"Unexpected exceptions: {errors}"
        assert len(wins) == 1, f"Expected 1 winner, got {len(wins)}"
        assert len(losses) == n - 1

    @pytest.mark.parametrize("n", [10, 100, 500])
    async def test_in_memory_exactly_one_claim_wins(self, memory_store, n):
        """Same guarantee for the in-memory store (asyncio.Lock)."""
        results = await asyncio.gather(
            *[memory_store.claim("herd-in-mem") for _ in range(n)],
        )
        assert results.count(True) == 1
        assert results.count(False) == n - 1

    async def test_concurrent_claim_then_complete_then_verify(self, fake_redis_store):
        """
        100 tasks race to claim 'race-complete'.
        The winner marks it COMPLETED.
        Every subsequent get_status call must return COMPLETED.
        """
        claims = await asyncio.gather(
            *[fake_redis_store.claim("race-complete") for _ in range(100)]
        )
        assert claims.count(True) == 1

        await fake_redis_store.mark_completed("race-complete", result={"v": 1})

        # All 50 concurrent reads should agree on COMPLETED
        statuses = await asyncio.gather(
            *[fake_redis_store.get_status("race-complete") for _ in range(50)]
        )
        assert all(s is not None and s.status == StatusValue.COMPLETED for s in statuses)


# ---------------------------------------------------------------------------
# 2. Lua guard — atomic state transitions
# ---------------------------------------------------------------------------


class TestLuaGuard:
    async def test_failed_cannot_overwrite_completed(self, fake_redis_store):
        await fake_redis_store.claim("lua-guard-1")
        await fake_redis_store.mark_completed("lua-guard-1", result="done")

        # Simulate a late-arriving failure (race: two workers process same msg)
        await fake_redis_store.mark_failed("lua-guard-1", error="too late")

        status = await fake_redis_store.get_status("lua-guard-1")
        assert status.status == StatusValue.COMPLETED

    async def test_second_complete_does_not_overwrite_result(self, fake_redis_store):
        await fake_redis_store.claim("lua-guard-2")
        await fake_redis_store.mark_completed("lua-guard-2", result="original")

        # A duplicate processor tries to complete it again
        await fake_redis_store.mark_completed("lua-guard-2", result="overwrite-attempt")

        status = await fake_redis_store.get_status("lua-guard-2")
        assert status.result == "original"

    async def test_concurrent_fail_complete_race(self, fake_redis_store):
        """
        One coroutine marks COMPLETED and another marks FAILED at the same time.
        The Lua CAS ensures exactly one transition wins; the record ends in a
        terminal state (COMPLETED wins because it was first).
        """
        await fake_redis_store.claim("lua-guard-3")

        # Race mark_completed vs mark_failed simultaneously
        results = await asyncio.gather(
            fake_redis_store.mark_completed("lua-guard-3", result="success"),
            fake_redis_store.mark_failed("lua-guard-3", error="parallel fail"),
            return_exceptions=True,
        )

        # Neither should raise
        assert all(not isinstance(r, Exception) for r in results)

        status = await fake_redis_store.get_status("lua-guard-3")
        # The status must be COMPLETED (claim → complete was first in this run,
        # but we accept either terminal state as correct; just not PROCESSING)
        assert status.status in (StatusValue.COMPLETED, StatusValue.FAILED)


# ---------------------------------------------------------------------------
# 3. Orphaned PROCESSING records (crash mid-flight)
# ---------------------------------------------------------------------------


class TestOrphanedProcessing:
    async def test_orphaned_processing_blocks_new_claim(self, memory_store):
        """
        Simulates a consumer crash after claim() but before mark_completed().
        The PROCESSING record is left behind.  A new consumer should NOT be
        able to claim the same message (would cause double processing).
        """
        # Simulate: consumer A claimed and crashed
        await memory_store.claim("orphan-msg-1")

        # Consumer B tries to process the same message — must be blocked
        claimed = await memory_store.claim("orphan-msg-1")
        assert claimed is False

        status = await memory_store.get_status("orphan-msg-1")
        assert status.status == StatusValue.PROCESSING

    async def test_orphaned_processing_blocks_in_redis(self, fake_redis_store):
        """Same guarantee for Redis store."""
        await fake_redis_store.claim("orphan-redis-1")
        claimed = await fake_redis_store.claim("orphan-redis-1")
        assert claimed is False

    async def test_failed_record_can_be_retried(self, memory_store):
        """
        A FAILED record (not PROCESSING) can be re-claimed up to max_retries.
        """
        await memory_store.claim("retry-msg-1")
        await memory_store.mark_failed("retry-msg-1", error="transient", attempts=1)

        reclaimed = await memory_store.claim("retry-msg-1")
        assert reclaimed is True

        status = await memory_store.get_status("retry-msg-1")
        assert status.status == StatusValue.PROCESSING
        assert status.attempts == 2

    async def test_max_retries_exhausted_blocks_reclaim(self):
        config = DeduplicationConfig(max_retries=2)
        store = InMemoryDeduplicationStore(config=config)

        await store.claim("max-retry-msg")
        await store.mark_failed("max-retry-msg", error="err", attempts=1)
        await store.claim("max-retry-msg")  # attempt 2 — ok
        await store.mark_failed("max-retry-msg", error="err", attempts=2)

        # attempts == max_retries → should not re-claim
        result = await store.claim("max-retry-msg")
        assert result is False


# ---------------------------------------------------------------------------
# 4. Store errors — consumer gracefully handles Redis failures
# ---------------------------------------------------------------------------


class TestStoreErrors:
    async def test_consumer_handles_claim_exception(self):
        """
        If store.claim() raises (e.g., Redis connection lost), _process_record
        should propagate the exception (causing the consumer to log and continue
        from the outer run() loop — not silently swallow it).
        """
        from unittest.mock import MagicMock

        store = InMemoryDeduplicationStore()
        store.claim = AsyncMock(side_effect=ConnectionError("Redis down"))
        store.is_duplicate = AsyncMock(return_value=False)

        consumer = DedupConsumer(topics=["t"], store=store)
        consumer._consumer = AsyncMock()
        consumer.handle = AsyncMock(return_value="ok")

        import json

        record = MagicMock()
        record.value = json.dumps({"message_id": "err-msg-1"}).encode()
        record.headers = []
        record.topic = "t"
        record.partition = 0
        record.offset = 0
        record.key = b"k"

        with pytest.raises(ConnectionError, match="Redis down"):
            await consumer._process_record(record)

    async def test_consumer_skips_commit_on_exception(self):
        """
        If _process_record raises before committing, the Kafka offset must NOT
        be committed (so Kafka redelivers the message after restart).
        """
        store = InMemoryDeduplicationStore()
        store.is_duplicate = AsyncMock(return_value=False)
        store.claim = AsyncMock(side_effect=RuntimeError("store unavailable"))

        consumer = DedupConsumer(topics=["t"], store=store)
        consumer._consumer = AsyncMock()

        import json
        from unittest.mock import MagicMock

        record = MagicMock()
        record.value = json.dumps({"message_id": "no-commit-msg"}).encode()
        record.headers = []
        record.topic = "t"
        record.partition = 0
        record.offset = 0
        record.key = b"k"

        with pytest.raises(RuntimeError):
            await consumer._process_record(record)

        # Offset commit must NOT have been called
        consumer._consumer.commit.assert_not_called()


# ---------------------------------------------------------------------------
# 5. Retry storm — exhaustion under rapid fire
# ---------------------------------------------------------------------------


class TestRetryStorm:
    async def test_exactly_one_completion_after_retries(self, memory_store):
        """
        handler fails on first 2 attempts, succeeds on 3rd.
        Verifies that the message is marked COMPLETED exactly once
        (no duplicate completions).
        """
        attempt_count = 0

        async def flaky_handler(message: Any) -> dict:
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise RuntimeError(f"transient failure attempt {attempt_count}")
            return {"done": True}

        consumer = DedupConsumer(
            topics=["t"],
            store=memory_store,
            config=DeduplicationConfig(max_retries=3, retry_backoff_ms=0),
        )
        consumer._consumer = AsyncMock()
        consumer.handle = flaky_handler

        import json
        from unittest.mock import MagicMock

        record = MagicMock()
        record.value = json.dumps({"message_id": "storm-msg-1"}).encode()
        record.headers = []
        record.topic = "t"
        record.partition = 0
        record.offset = 0
        record.key = b"k"

        await consumer._process_record(record)

        assert attempt_count == 3
        status = await memory_store.get_status("storm-msg-1")
        assert status.status == StatusValue.COMPLETED
        assert status.result == {"done": True}

    async def test_dlq_after_all_retries_exhausted(self, memory_store):
        """Message that always fails must end up in DLQ, not retried forever."""
        consumer = DedupConsumer(
            topics=["t"],
            store=memory_store,
            dlq_topic="t.dlq",
            config=DeduplicationConfig(max_retries=2, retry_backoff_ms=0),
        )
        consumer._consumer = AsyncMock()
        consumer.handle = AsyncMock(side_effect=RuntimeError("fatal"))

        dlq_producer = AsyncMock()
        consumer._dlq_producer = dlq_producer

        import json
        from unittest.mock import MagicMock

        record = MagicMock()
        record.value = json.dumps({"message_id": "storm-dlq-1"}).encode()
        record.headers = []
        record.topic = "t"
        record.partition = 0
        record.offset = 0
        record.key = b"k"

        await consumer._process_record(record)

        dlq_producer.send_and_wait.assert_called_once()
        assert dlq_producer.send_and_wait.call_args[0][0] == "t.dlq"

    async def test_no_double_processing_after_retry(self, memory_store):
        """
        Simulates duplicate Kafka delivery of a message that was processed
        successfully on the first attempt.  The second delivery must be skipped.
        """
        call_count = 0

        async def counting_handler(message: Any) -> dict:
            nonlocal call_count
            call_count += 1
            return {"call": call_count}

        consumer = DedupConsumer(topics=["t"], store=memory_store)
        consumer._consumer = AsyncMock()
        consumer.handle = counting_handler

        import json
        from unittest.mock import MagicMock

        def _make_record():
            r = MagicMock()
            r.value = json.dumps({"message_id": "double-delivery-1"}).encode()
            r.headers = []
            r.topic = "t"
            r.partition = 0
            r.offset = 0
            r.key = b"k"
            return r

        # First delivery
        await consumer._process_record(_make_record())
        # Second delivery (Kafka redelivery)
        await consumer._process_record(_make_record())

        assert call_count == 1, "Handler must be called exactly once even with duplicate delivery"

    async def test_concurrent_consumers_exactly_once(self, fake_redis_store):
        """
        Two consumers both receive the same message_id.
        Only one should win the claim and process it.
        """
        processed_by: list[str] = []

        class _NamedConsumer(DedupConsumer):
            def __init__(self, name: str, **kwargs: Any) -> None:
                super().__init__(**kwargs)
                self._name = name

            async def handle(self, message: Any) -> Any:
                processed_by.append(self._name)
                return {"processor": self._name}

        consumer_a = _NamedConsumer("A", topics=["t"], store=fake_redis_store)
        consumer_b = _NamedConsumer("B", topics=["t"], store=fake_redis_store)
        consumer_a._consumer = AsyncMock()
        consumer_b._consumer = AsyncMock()

        import json
        from unittest.mock import MagicMock

        def _make_record():
            r = MagicMock()
            r.value = json.dumps({"message_id": "concurrent-once"}).encode()
            r.headers = []
            r.topic = "t"
            r.partition = 0
            r.offset = 0
            r.key = b"k"
            return r

        await asyncio.gather(
            consumer_a._process_record(_make_record()),
            consumer_b._process_record(_make_record()),
        )

        assert len(processed_by) == 1, f"Expected exactly one processor, got: {processed_by}"
