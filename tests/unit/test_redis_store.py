"""
Unit tests for RedisDeduplicationStore using fakeredis (no real Redis needed).
"""

import fakeredis
import fakeredis.aioredis
import pytest

from src.dedup import RedisDeduplicationStore
from src.dedup.models import StatusValue


@pytest.fixture
async def store() -> RedisDeduplicationStore:
    # version=(7, 2) + lupa installed → enables Lua (EVAL) support in fakeredis
    server = fakeredis.FakeServer(version=(7, 2))
    client = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    s = RedisDeduplicationStore(client)
    await s.connect()
    yield s
    await s.close()


# ---------------------------------------------------------------------------
# claim()
# ---------------------------------------------------------------------------


class TestClaim:
    async def test_first_claim_succeeds(self, store):
        assert await store.claim("msg-1") is True

    async def test_second_claim_is_false(self, store):
        await store.claim("msg-1")
        assert await store.claim("msg-1") is False

    async def test_different_ids_independent(self, store):
        assert await store.claim("msg-a") is True
        assert await store.claim("msg-b") is True

    async def test_claim_writes_processing_status(self, store):
        await store.claim("msg-1")
        status = await store.get_status("msg-1")
        assert status is not None
        assert status.status == StatusValue.PROCESSING
        assert status.message_id == "msg-1"


# ---------------------------------------------------------------------------
# is_duplicate()
# ---------------------------------------------------------------------------


class TestIsDuplicate:
    async def test_unknown_not_duplicate(self, store):
        assert await store.is_duplicate("ghost") is False

    async def test_claimed_is_duplicate(self, store):
        await store.claim("msg-1")
        assert await store.is_duplicate("msg-1") is True


# ---------------------------------------------------------------------------
# mark_completed()
# ---------------------------------------------------------------------------


class TestMarkCompleted:
    async def test_transitions_to_completed(self, store):
        await store.claim("msg-1")
        await store.mark_completed("msg-1", result={"order_id": 99})
        status = await store.get_status("msg-1")
        assert status.status == StatusValue.COMPLETED
        assert status.result == {"order_id": 99}

    async def test_completed_without_result(self, store):
        await store.claim("msg-1")
        await store.mark_completed("msg-1")
        status = await store.get_status("msg-1")
        assert status.status == StatusValue.COMPLETED
        assert status.result is None

    async def test_completed_is_idempotent(self, store):
        """Calling mark_completed twice should not raise — second call is ignored."""
        await store.claim("msg-1")
        await store.mark_completed("msg-1", result="first")
        await store.mark_completed("msg-1", result="second")  # Lua returns 0, no raise
        status = await store.get_status("msg-1")
        assert status.result == "first"  # original result preserved


# ---------------------------------------------------------------------------
# mark_failed()
# ---------------------------------------------------------------------------


class TestMarkFailed:
    async def test_transitions_to_failed(self, store):
        await store.claim("msg-1")
        await store.mark_failed("msg-1", error="timeout", attempts=1)
        status = await store.get_status("msg-1")
        assert status.status == StatusValue.FAILED
        assert status.error == "timeout"

    async def test_cannot_overwrite_completed_with_failed(self, store):
        """Lua script should block this transition."""
        await store.claim("msg-1")
        await store.mark_completed("msg-1", result="done")
        await store.mark_failed("msg-1", error="too late", attempts=2)
        status = await store.get_status("msg-1")
        assert status.status == StatusValue.COMPLETED  # unchanged


# ---------------------------------------------------------------------------
# get_status()
# ---------------------------------------------------------------------------


class TestGetStatus:
    async def test_none_for_unknown_id(self, store):
        assert await store.get_status("never-seen") is None

    async def test_returns_full_status(self, store):
        await store.claim("msg-1")
        status = await store.get_status("msg-1")
        assert status.message_id == "msg-1"
        assert status.attempts == 1
        assert status.created_at is not None


# ---------------------------------------------------------------------------
# delete()
# ---------------------------------------------------------------------------


class TestDelete:
    async def test_delete_removes_key(self, store):
        await store.claim("msg-1")
        await store.delete("msg-1")
        assert await store.is_duplicate("msg-1") is False

    async def test_delete_nonexistent_safe(self, store):
        await store.delete("ghost")  # should not raise


# ---------------------------------------------------------------------------
# TTL (fakeredis does not enforce TTL by default, but we verify the key is set)
# ---------------------------------------------------------------------------


class TestTTL:
    async def test_claim_sets_ttl(self, store):
        """Verify that the underlying key has a TTL set."""
        await store.claim("msg-1")
        ttl = await store._client.pttl("dedup:msg-1")
        assert ttl > 0
