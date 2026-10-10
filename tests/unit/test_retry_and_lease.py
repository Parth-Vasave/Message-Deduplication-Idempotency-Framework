"""
Regression tests for retry, crash-recovery and completion semantics.

Every store-level test runs against both backends so the in-memory store can't
mask a Redis-only bug again.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import fakeredis
import fakeredis.aioredis
import pytest

from src.consumer.dedup_consumer import DedupConsumer
from src.dedup import InMemoryDeduplicationStore, RedisDeduplicationStore
from src.dedup.models import DeduplicationConfig, StatusValue
from src.dedup.store import DeduplicationStore
from src.idempotency.guard import idempotent


def _make_store(kind: str, **config_kwargs) -> DeduplicationStore:
    config = DeduplicationConfig(**config_kwargs)
    if kind == "memory":
        return InMemoryDeduplicationStore(config=config)
    server = fakeredis.FakeServer(version=(7, 2))
    client = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    return RedisDeduplicationStore(client, config=config)


@pytest.fixture(params=["memory", "redis"])
def make_store(request):
    return lambda **kw: _make_store(request.param, **kw)


def _record(message_id: str) -> MagicMock:
    record = MagicMock()
    record.value = json.dumps({"message_id": message_id}).encode()
    record.headers = []
    record.topic = "t"
    record.partition = 0
    record.offset = 0
    record.key = b"k"
    return record


# ---------------------------------------------------------------------------
# FAILED records are retriable on every backend
# ---------------------------------------------------------------------------


class TestFailedReclaim:
    async def test_failed_record_can_be_reclaimed(self, make_store):
        store = make_store()
        await store.claim("m")
        await store.mark_failed("m", error="transient", attempts=1)

        assert await store.claim("m") is True
        status = await store.get_status("m")
        assert status.status == StatusValue.PROCESSING
        assert status.attempts == 2
        assert status.error is None

    async def test_exhausted_retries_block_reclaim(self, make_store):
        store = make_store(max_retries=2)
        await store.claim("m")
        await store.mark_failed("m", error="e", attempts=1)
        assert await store.claim("m") is True
        await store.mark_failed("m", error="e", attempts=2)

        assert await store.claim("m") is False

    async def test_retry_failed_off_blocks_reclaim(self, make_store):
        store = make_store(retry_failed=False)
        await store.claim("m")
        await store.mark_failed("m", error="e", attempts=1)

        assert await store.claim("m") is False


# ---------------------------------------------------------------------------
# PROCESSING lease — crash recovery
# ---------------------------------------------------------------------------


class TestProcessingLease:
    async def test_live_lease_blocks_claim(self, make_store):
        store = make_store(processing_timeout_seconds=300)
        await store.claim("m")
        assert await store.claim("m") is False

    async def test_expired_lease_allows_reclaim(self, make_store):
        """Owner crashed after claim(); once the lease lapses, the message is claimable."""
        store = make_store(processing_timeout_seconds=0)
        await store.claim("m")

        assert await store.claim("m") is True
        status = await store.get_status("m")
        assert status.status == StatusValue.PROCESSING
        assert status.attempts == 2

    async def test_claim_exposes_lease(self, make_store):
        store = make_store(processing_timeout_seconds=300)
        await store.claim("m")
        status = await store.get_status("m")
        assert status.lease_expires_at is not None
        assert status.lease_expires_at > status.created_at

    async def test_completed_record_never_reclaimed(self, make_store):
        store = make_store(processing_timeout_seconds=0)
        await store.claim("m")
        await store.mark_completed("m", result="done")

        assert await store.claim("m") is False
        status = await store.get_status("m")
        assert status.status == StatusValue.COMPLETED
        assert status.lease_expires_at is None


# ---------------------------------------------------------------------------
# DedupConsumer end-to-end over each backend
# ---------------------------------------------------------------------------


class TestConsumerRetries:
    async def test_transient_failures_are_retried(self, make_store):
        store = make_store(max_retries=3)
        consumer = DedupConsumer(
            topics=["t"],
            store=store,
            config=DeduplicationConfig(max_retries=3, retry_backoff_ms=0),
        )
        consumer._consumer = AsyncMock()
        consumer.handle = AsyncMock(side_effect=[OSError("1"), OSError("2"), {"ok": True}])
        consumer._send_to_dlq = AsyncMock()

        await consumer._process_record(_record("m"))

        assert consumer.handle.await_count == 3
        consumer._send_to_dlq.assert_not_called()
        status = await store.get_status("m")
        assert status.status == StatusValue.COMPLETED
        assert status.result == {"ok": True}

    async def test_redelivery_after_crash_is_processed(self, make_store):
        store = make_store(processing_timeout_seconds=0)
        await store.claim("m")  # previous consumer claimed, then crashed

        consumer = DedupConsumer(topics=["t"], store=store)
        consumer._consumer = AsyncMock()
        consumer.handle = AsyncMock(return_value="recovered")

        await consumer._process_record(_record("m"))

        consumer.handle.assert_awaited_once()
        assert (await store.get_status("m")).status == StatusValue.COMPLETED

    async def test_in_flight_message_is_skipped(self, make_store):
        store = make_store(processing_timeout_seconds=300)
        await store.claim("m")

        consumer = DedupConsumer(topics=["t"], store=store)
        consumer._consumer = AsyncMock()
        consumer.handle = AsyncMock()

        await consumer._process_record(_record("m"))

        consumer.handle.assert_not_called()
        consumer._consumer.commit.assert_awaited_once()


# ---------------------------------------------------------------------------
# A store failure after a successful handler must not re-run the handler
# ---------------------------------------------------------------------------


class TestMarkCompletedFailure:
    async def test_consumer_does_not_rerun_handle(self):
        store = InMemoryDeduplicationStore()
        store.mark_completed = AsyncMock(side_effect=ConnectionError("Redis down"))
        consumer = DedupConsumer(
            topics=["t"],
            store=store,
            config=DeduplicationConfig(max_retries=3, retry_backoff_ms=0),
        )
        consumer._consumer = AsyncMock()
        consumer.handle = AsyncMock(return_value={"charged": 100})
        consumer._send_to_dlq = AsyncMock()

        await consumer._process_record(_record("m"))

        consumer.handle.assert_awaited_once()
        consumer._send_to_dlq.assert_not_called()
        consumer._consumer.commit.assert_awaited_once()

    async def test_unserialisable_result_does_not_rerun_handle(self):
        store = _make_store("redis")
        consumer = DedupConsumer(
            topics=["t"],
            store=store,
            config=DeduplicationConfig(max_retries=3, retry_backoff_ms=0),
        )
        consumer._consumer = AsyncMock()
        consumer.handle = AsyncMock(return_value=object())  # json.dumps raises TypeError

        await consumer._process_record(_record("m"))

        consumer.handle.assert_awaited_once()
        consumer._consumer.commit.assert_awaited_once()

    async def test_decorator_does_not_rerun_func(self):
        store = InMemoryDeduplicationStore()
        store.mark_completed = AsyncMock(side_effect=ConnectionError("Redis down"))
        calls = 0

        @idempotent(store=store, config=DeduplicationConfig(retry_backoff_ms=0))
        async def charge(key: str) -> str:
            nonlocal calls
            calls += 1
            return "charged"

        assert await charge("pay-1") == "charged"
        assert calls == 1
