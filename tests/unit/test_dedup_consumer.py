"""Unit tests for DedupConsumer (no Kafka — uses mocks)."""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.consumer.dedup_consumer import DedupConsumer, _extract_message_id
from src.dedup import InMemoryDeduplicationStore
from src.dedup.models import StatusValue


def _make_record(
    value: bytes | None = None,
    headers: list | None = None,
    topic: str = "test-topic",
    partition: int = 0,
    offset: int = 0,
):
    """Build a minimal ConsumerRecord-like object."""
    record = MagicMock()
    record.value = value
    record.headers = headers or []
    record.topic = topic
    record.partition = partition
    record.offset = offset
    record.key = b"key"
    return record


@pytest.fixture
def store():
    return InMemoryDeduplicationStore()


@pytest.fixture
def consumer(store):
    c = DedupConsumer(topics=["test"], store=store)
    c._consumer = AsyncMock()  # mock the aiokafka consumer
    return c


# ---------------------------------------------------------------------------
# _extract_message_id
# ---------------------------------------------------------------------------


class TestExtractMessageId:
    def test_from_header(self):
        record = _make_record(headers=[("X-Message-Id", b"hdr-123")])
        assert _extract_message_id(record) == "hdr-123"

    def test_from_json_payload(self):
        import json

        record = _make_record(value=json.dumps({"message_id": "json-456"}).encode())
        assert _extract_message_id(record) == "json-456"

    def test_from_id_field(self):
        import json

        record = _make_record(value=json.dumps({"id": "id-789"}).encode())
        assert _extract_message_id(record) == "id-789"

    def test_header_takes_priority_over_payload(self):
        import json

        record = _make_record(
            value=json.dumps({"message_id": "payload-id"}).encode(),
            headers=[("X-Message-Id", b"header-id")],
        )
        assert _extract_message_id(record) == "header-id"

    def test_missing_returns_none(self):
        record = _make_record(value=b'{"foo": "bar"}')
        assert _extract_message_id(record) is None

    def test_invalid_json_returns_none(self):
        record = _make_record(value=b"not-json")
        assert _extract_message_id(record) is None


# ---------------------------------------------------------------------------
# _process_record
# ---------------------------------------------------------------------------


class TestProcessRecord:
    async def test_new_message_is_processed(self, consumer, store):
        import json

        consumer.handle = AsyncMock(return_value={"ok": True})

        record = _make_record(value=json.dumps({"message_id": "msg-1", "data": "x"}).encode())
        await consumer._process_record(record)

        consumer.handle.assert_called_once()
        status = await store.get_status("msg-1")
        assert status.status == StatusValue.COMPLETED
        assert status.result == {"ok": True}

    async def test_duplicate_is_skipped(self, consumer, store):
        import json

        # Pre-claim the message
        await store.claim("msg-dup")
        await store.mark_completed("msg-dup", result="prior")

        consumer.handle = AsyncMock()
        record = _make_record(value=json.dumps({"message_id": "msg-dup"}).encode())
        await consumer._process_record(record)

        consumer.handle.assert_not_called()
        consumer._consumer.commit.assert_called_once()

    async def test_handle_failure_marks_failed(self, consumer, store):
        import json

        from src.dedup.models import DeduplicationConfig

        consumer._config = DeduplicationConfig(max_retries=1, retry_backoff_ms=0)
        consumer.handle = AsyncMock(side_effect=RuntimeError("db down"))
        consumer._dlq_producer = None
        consumer._dlq_topic = None

        record = _make_record(value=json.dumps({"message_id": "msg-err"}).encode())
        await consumer._process_record(record)

        status = await store.get_status("msg-err")
        assert status.status == StatusValue.FAILED

    async def test_no_message_id_still_processes(self, consumer, store):
        """Messages without IDs are processed without dedup (best-effort)."""
        consumer.handle = AsyncMock(return_value="done")
        record = _make_record(value=b'{"no_id": true}')
        await consumer._process_record(record)
        consumer.handle.assert_called_once()

    async def test_failed_message_is_retried(self, consumer, store):
        # Mark as failed with attempt 1 (below max_retries=3)
        await store.claim("msg-retry")
        await store.mark_failed("msg-retry", error="transient", attempts=1)

        consumer.handle = AsyncMock(return_value={"retried": True})
        record = _make_record(value=json.dumps({"message_id": "msg-retry"}).encode())
        await consumer._process_record(record)

        status = await store.get_status("msg-retry")
        assert status.status == StatusValue.COMPLETED

    async def test_message_sent_to_dlq_after_exhausted_retries(self, consumer, store):
        from src.dedup.models import DeduplicationConfig

        consumer._config = DeduplicationConfig(max_retries=1, retry_backoff_ms=0)
        consumer.handle = AsyncMock(side_effect=RuntimeError("fatal"))

        dlq_producer = AsyncMock()
        consumer._dlq_producer = dlq_producer
        consumer._dlq_topic = "test.dlq"

        record = _make_record(value=json.dumps({"message_id": "msg-dlq"}).encode())
        await consumer._process_record(record)

        dlq_producer.send_and_wait.assert_called_once()
        call_args = dlq_producer.send_and_wait.call_args
        assert call_args[0][0] == "test.dlq"

    async def test_run_raises_if_not_started(self, consumer):
        consumer._consumer = None
        with pytest.raises(RuntimeError, match="Call start()"):
            await consumer.run()

    async def test_stop_closes_store(self, consumer, store):
        consumer._consumer = AsyncMock()
        consumer._dlq_producer = None

        closed = []

        async def track_close():
            closed.append(True)

        store.close = track_close
        await consumer.stop()

        assert closed == [True]
        assert consumer._running is False
