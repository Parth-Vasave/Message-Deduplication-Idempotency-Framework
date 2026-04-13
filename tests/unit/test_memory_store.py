"""Unit tests for InMemoryDeduplicationStore."""

import asyncio
import pytest

from src.dedup import InMemoryDeduplicationStore
from src.dedup.models import DeduplicationConfig, StatusValue


@pytest.fixture
def store() -> InMemoryDeduplicationStore:
    return InMemoryDeduplicationStore()


# ---------------------------------------------------------------------------
# claim()
# ---------------------------------------------------------------------------

class TestClaim:
    async def test_first_claim_succeeds(self, store):
        result = await store.claim("msg-1")
        assert result is True

    async def test_second_claim_is_duplicate(self, store):
        await store.claim("msg-1")
        result = await store.claim("msg-1")
        assert result is False

    async def test_different_ids_both_succeed(self, store):
        assert await store.claim("msg-a") is True
        assert await store.claim("msg-b") is True

    async def test_claim_sets_processing_status(self, store):
        await store.claim("msg-1")
        status = await store.get_status("msg-1")
        assert status is not None
        assert status.status == StatusValue.PROCESSING

    async def test_concurrent_claims_only_one_wins(self, store):
        """10 coroutines racing to claim the same id — exactly 1 should win."""
        results = await asyncio.gather(
            *[store.claim("msg-race") for _ in range(10)]
        )
        assert results.count(True) == 1
        assert results.count(False) == 9


# ---------------------------------------------------------------------------
# is_duplicate()
# ---------------------------------------------------------------------------

class TestIsDuplicate:
    async def test_unknown_id_is_not_duplicate(self, store):
        assert await store.is_duplicate("unknown") is False

    async def test_claimed_id_is_duplicate(self, store):
        await store.claim("msg-1")
        assert await store.is_duplicate("msg-1") is True

    async def test_completed_id_is_duplicate(self, store):
        await store.claim("msg-1")
        await store.mark_completed("msg-1", result={"order_id": 42})
        assert await store.is_duplicate("msg-1") is True


# ---------------------------------------------------------------------------
# mark_completed()
# ---------------------------------------------------------------------------

class TestMarkCompleted:
    async def test_transitions_to_completed(self, store):
        await store.claim("msg-1")
        await store.mark_completed("msg-1", result={"ok": True})
        status = await store.get_status("msg-1")
        assert status.status == StatusValue.COMPLETED
        assert status.result == {"ok": True}

    async def test_completed_is_terminal(self, store):
        await store.claim("msg-1")
        await store.mark_completed("msg-1")
        status = await store.get_status("msg-1")
        assert status.is_terminal() is True

    async def test_completed_without_result(self, store):
        await store.claim("msg-1")
        await store.mark_completed("msg-1")
        status = await store.get_status("msg-1")
        assert status.result is None


# ---------------------------------------------------------------------------
# mark_failed()
# ---------------------------------------------------------------------------

class TestMarkFailed:
    async def test_transitions_to_failed(self, store):
        await store.claim("msg-1")
        await store.mark_failed("msg-1", error="DB timeout", attempts=1)
        status = await store.get_status("msg-1")
        assert status.status == StatusValue.FAILED
        assert status.error == "DB timeout"
        assert status.attempts == 1

    async def test_failed_is_not_terminal(self, store):
        await store.claim("msg-1")
        await store.mark_failed("msg-1", error="oops")
        status = await store.get_status("msg-1")
        assert status.is_terminal() is False
        assert status.is_failed() is True


# ---------------------------------------------------------------------------
# Retry after failure
# ---------------------------------------------------------------------------

class TestRetryAfterFailure:
    async def test_failed_message_can_be_reclaimed(self, store):
        """A FAILED message should be re-claimable (retry_failed=True by default)."""
        await store.claim("msg-1")
        await store.mark_failed("msg-1", error="transient", attempts=1)

        result = await store.claim("msg-1")
        assert result is True

        status = await store.get_status("msg-1")
        assert status.status == StatusValue.PROCESSING
        assert status.attempts == 2

    async def test_max_retries_blocks_reclaim(self):
        config = DeduplicationConfig(max_retries=2)
        store = InMemoryDeduplicationStore(config=config)

        await store.claim("msg-1")
        await store.mark_failed("msg-1", error="err", attempts=1)
        await store.claim("msg-1")   # retry 1 — should succeed
        await store.mark_failed("msg-1", error="err", attempts=2)

        # attempts == max_retries, should not re-claim
        result = await store.claim("msg-1")
        assert result is False

    async def test_completed_message_cannot_be_reclaimed(self, store):
        await store.claim("msg-1")
        await store.mark_completed("msg-1")
        result = await store.claim("msg-1")
        assert result is False


# ---------------------------------------------------------------------------
# delete() / size()
# ---------------------------------------------------------------------------

class TestDelete:
    async def test_delete_removes_record(self, store):
        await store.claim("msg-1")
        await store.delete("msg-1")
        assert await store.is_duplicate("msg-1") is False
        assert store.size() == 0

    async def test_delete_nonexistent_is_safe(self, store):
        await store.delete("does-not-exist")  # should not raise

    async def test_size_tracks_records(self, store):
        assert store.size() == 0
        await store.claim("msg-1")
        await store.claim("msg-2")
        assert store.size() == 2
        await store.delete("msg-1")
        assert store.size() == 1
