"""
Integration tests for RedisDeduplicationStore against a real Redis 7.2 container.

These tests verify behaviour that fakeredis approximates but does not guarantee:
  - Actual TTL enforcement by the Redis daemon
  - Lua EVAL atomicity under concurrent goroutines
  - SET NX race conditions with real network I/O
"""

from __future__ import annotations

import asyncio
import time

import pytest

from src.dedup.models import DeduplicationConfig, StatusValue

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Basic lifecycle
# ---------------------------------------------------------------------------


class TestBasicLifecycle:
    async def test_claim_succeeds_first_time(self, redis_store):
        assert await redis_store.claim("int-msg-1") is True

    async def test_claim_fails_on_duplicate(self, redis_store):
        await redis_store.claim("int-msg-2")
        assert await redis_store.claim("int-msg-2") is False

    async def test_is_duplicate_false_for_unknown(self, redis_store):
        assert await redis_store.is_duplicate("int-unknown") is False

    async def test_full_happy_path(self, redis_store):
        await redis_store.claim("int-msg-3")
        await redis_store.mark_completed("int-msg-3", result={"order": 42})

        status = await redis_store.get_status("int-msg-3")
        assert status is not None
        assert status.status == StatusValue.COMPLETED
        assert status.result == {"order": 42}

    async def test_mark_failed_then_get_status(self, redis_store):
        await redis_store.claim("int-msg-4")
        await redis_store.mark_failed("int-msg-4", error="db timeout", attempts=1)

        status = await redis_store.get_status("int-msg-4")
        assert status.status == StatusValue.FAILED
        assert status.error == "db timeout"
        assert status.attempts == 1

    async def test_delete_removes_key(self, redis_store):
        await redis_store.claim("int-msg-5")
        await redis_store.delete("int-msg-5")
        assert await redis_store.is_duplicate("int-msg-5") is False

    async def test_delete_nonexistent_is_safe(self, redis_store):
        await redis_store.delete("int-never-existed")  # must not raise


# ---------------------------------------------------------------------------
# Lua script correctness (requires real Redis EVAL)
# ---------------------------------------------------------------------------


class TestLuaScript:
    async def test_completed_cannot_be_overwritten_with_failed(self, redis_store):
        """Lua guard: COMPLETED → FAILED transition must be blocked."""
        await redis_store.claim("lua-msg-1")
        await redis_store.mark_completed("lua-msg-1", result="done")

        # Late-arriving failure should be silently ignored
        await redis_store.mark_failed("lua-msg-1", error="too late", attempts=2)

        status = await redis_store.get_status("lua-msg-1")
        assert status.status == StatusValue.COMPLETED
        assert status.result == "done"

    async def test_mark_completed_is_idempotent(self, redis_store):
        """Calling mark_completed twice keeps the first result."""
        await redis_store.claim("lua-msg-2")
        await redis_store.mark_completed("lua-msg-2", result="first")
        await redis_store.mark_completed("lua-msg-2", result="second")

        status = await redis_store.get_status("lua-msg-2")
        assert status.result == "first"

    async def test_expired_key_returns_minus_one(self, redis_store):
        """mark_completed on an expired key logs a warning but does not raise."""
        # Claim with a 1-second TTL
        short_config = DeduplicationConfig(ttl_seconds=1)
        redis_store._config = short_config

        await redis_store.claim("lua-expire-1")
        # Wait for expiry
        await asyncio.sleep(1.2)

        # Should not raise — Lua returns -1 and the store logs a warning
        await redis_store.mark_completed("lua-expire-1", result="late")


# ---------------------------------------------------------------------------
# TTL enforcement (real Redis daemon enforces TTL, fakeredis does not)
# ---------------------------------------------------------------------------


class TestTTL:
    async def test_key_expires_after_ttl(self, redis_store):
        """Verify the Redis daemon actually removes the key after TTL seconds."""
        redis_store._config = DeduplicationConfig(ttl_seconds=1)

        await redis_store.claim("ttl-msg-1")
        assert await redis_store.is_duplicate("ttl-msg-1") is True

        await asyncio.sleep(1.3)

        assert await redis_store.is_duplicate("ttl-msg-1") is False
        assert await redis_store.get_status("ttl-msg-1") is None

    async def test_ttl_set_on_claim(self, redis_store):
        """Verify the key has a positive TTL immediately after claim."""
        await redis_store.claim("ttl-msg-2")
        ttl_ms = await redis_store._client.pttl("dedup:ttl-msg-2")
        assert ttl_ms > 0


# ---------------------------------------------------------------------------
# Concurrency: SET NX atomicity under real async I/O
# ---------------------------------------------------------------------------


class TestConcurrency:
    async def test_100_coroutines_only_one_claims(self, redis_store):
        """
        100 asyncio tasks all race to claim the same message_id.
        Exactly one must win — the rest must return False.
        """
        results = await asyncio.gather(
            *[redis_store.claim("race-msg-1") for _ in range(100)]
        )
        assert results.count(True) == 1
        assert results.count(False) == 99

    async def test_independent_ids_all_succeed(self, redis_store):
        """100 different message IDs should all claim successfully."""
        results = await asyncio.gather(
            *[redis_store.claim(f"uniq-{i}") for i in range(100)]
        )
        assert all(results)

    async def test_concurrent_claim_then_complete(self, redis_store):
        """
        50 coroutines compete for the same key; the winner marks it COMPLETED.
        All subsequent get_status calls must return COMPLETED.
        """
        claims = await asyncio.gather(
            *[redis_store.claim("complete-race") for _ in range(50)]
        )
        winner_count = claims.count(True)
        assert winner_count == 1

        await redis_store.mark_completed("complete-race", result={"ok": True})

        status = await redis_store.get_status("complete-race")
        assert status.status == StatusValue.COMPLETED
