"""Unit tests for IdempotencyGuard and @idempotent decorator."""

import pytest

from src.dedup import InMemoryDeduplicationStore
from src.dedup.models import StatusValue
from src.idempotency.guard import IdempotencyGuard, idempotent


@pytest.fixture
def store() -> InMemoryDeduplicationStore:
    return InMemoryDeduplicationStore()


# ---------------------------------------------------------------------------
# IdempotencyGuard
# ---------------------------------------------------------------------------


class TestIdempotencyGuard:
    async def test_first_use_processes(self, store):
        async with IdempotencyGuard(store, "key-1") as guard:
            assert guard.already_processed is False
            guard.result = {"done": True}

        status = await store.get_status("key-1")
        assert status.status == StatusValue.COMPLETED
        assert status.result == {"done": True}

    async def test_second_use_skips(self, store):
        async with IdempotencyGuard(store, "key-1") as guard:
            guard.result = "first"

        async with IdempotencyGuard(store, "key-1") as guard:
            assert guard.already_processed is True
            assert guard.previous_result == "first"

    async def test_exception_marks_failed(self, store):
        with pytest.raises(ValueError, match="boom"):
            async with IdempotencyGuard(store, "key-1"):
                raise ValueError("boom")

        status = await store.get_status("key-1")
        assert status.status == StatusValue.FAILED
        assert "boom" in status.error

    async def test_in_flight_claim_lost(self, store):
        """Simulate two concurrent guards racing for the same key."""
        # First guard claims but hasn't finished
        await store.claim("key-1")

        async with IdempotencyGuard(store, "key-1") as guard:
            # Claim was already taken → treated as already processed
            assert guard.already_processed is True


# ---------------------------------------------------------------------------
# @idempotent decorator
# ---------------------------------------------------------------------------


class TestIdempotentDecorator:
    async def test_executes_on_first_call(self, store):
        call_count = 0

        @idempotent(store=store, key_fn=lambda msg_id, _: msg_id)
        async def do_work(msg_id: str, payload: dict) -> dict:
            nonlocal call_count
            call_count += 1
            return {"processed": payload}

        result = await do_work("msg-1", {"data": 42})
        assert call_count == 1
        assert result == {"processed": {"data": 42}}

    async def test_returns_cached_result_on_duplicate(self, store):
        call_count = 0

        @idempotent(store=store, key_fn=lambda msg_id, _: msg_id)
        async def do_work(msg_id: str, payload: dict) -> dict:
            nonlocal call_count
            call_count += 1
            return {"value": payload.get("v")}

        await do_work("msg-1", {"v": 99})
        result = await do_work("msg-1", {"v": 99})

        assert call_count == 1  # only executed once
        assert result == {"value": 99}  # cached result returned

    async def test_default_key_from_first_arg(self, store):
        @idempotent(store=store)
        async def do_work(message_id: str) -> str:
            return f"done-{message_id}"

        result = await do_work("my-key")
        assert result == "done-my-key"

        status = await store.get_status("my-key")
        assert status.status == StatusValue.COMPLETED

    async def test_raises_after_exhausting_retries(self, store):
        from src.dedup.models import DeduplicationConfig

        config = DeduplicationConfig(max_retries=2, retry_backoff_ms=0)

        @idempotent(store=store, key_fn=lambda k: k, config=config)
        async def always_fails(key: str) -> None:
            raise RuntimeError("always broken")

        with pytest.raises(RuntimeError, match="always broken"):
            await always_fails("key-fail")

    async def test_no_key_fn_and_no_args_raises(self, store):
        @idempotent(store=store)
        async def do_work(**kwargs) -> None:
            pass

        with pytest.raises(ValueError, match="cannot derive idempotency key"):
            await do_work(foo="bar")
