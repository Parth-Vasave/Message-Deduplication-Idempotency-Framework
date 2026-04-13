"""
DedupConsumer — aiokafka consumer with full deduplication lifecycle.

Lifecycle per message:
  1. Receive message from Kafka (poll)
  2. Extract message_id (from header "X-Message-Id" or payload["message_id"])
  3. is_duplicate? → YES: skip, commit offset
  4. claim() atomically (SET NX)
  5. Execute handle() (your business logic)
  6. mark_completed(result) → commit offset
  7. On error → mark_failed() → retry / send to DLQ
"""

import asyncio
import logging
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord

from src.config import settings
from src.dedup.models import DeduplicationConfig
from src.dedup.store import DeduplicationStore
from src.metrics.dedup_metrics import DeduplicationMetrics, metrics as _default_metrics

from .base import BaseConsumer

logger = logging.getLogger(__name__)


def _extract_message_id(record: ConsumerRecord) -> str | None:
    """
    Try to pull the message_id from:
      1. Kafka header  ``X-Message-Id``
      2. Decoded JSON payload field ``message_id``

    Returns None if neither is present.
    """
    for key, value in (record.headers or []):
        if key == "X-Message-Id" and value:
            return value.decode()

    # Attempt JSON decode — callers can override _parse_payload for other formats
    import json
    if not record.value:
        return None
    try:
        payload = json.loads(record.value)
        if isinstance(payload, dict):
            return payload.get("message_id") or payload.get("id")
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
        pass

    return None


class DedupConsumer(BaseConsumer):
    """
    Kafka consumer with built-in deduplication.

    Args:
        topics:       List of Kafka topics to subscribe to.
        store:        DeduplicationStore (Redis or in-memory).
        group_id:     Kafka consumer group id (defaults to settings).
        bootstrap_servers: Kafka brokers (defaults to settings).
        dlq_topic:    If set, failed messages beyond max_retries go here.
        config:       DeduplicationConfig (TTL, retries, backoff).
        kafka_kwargs: Extra kwargs forwarded to AIOKafkaConsumer.
    """

    def __init__(
        self,
        topics: list[str],
        store: DeduplicationStore,
        group_id: str | None = None,
        bootstrap_servers: str | None = None,
        dlq_topic: str | None = None,
        config: DeduplicationConfig | None = None,
        metrics: DeduplicationMetrics | None = None,
        **kafka_kwargs: Any,
    ) -> None:
        self._topics = topics
        self._store = store
        self._config = config or DeduplicationConfig()
        self._dlq_topic = dlq_topic
        self._metrics = metrics or _default_metrics

        self._bootstrap = bootstrap_servers or settings.kafka_bootstrap_servers
        self._group_id = group_id or settings.kafka_group_id

        self._consumer: AIOKafkaConsumer | None = None
        self._dlq_producer: AIOKafkaProducer | None = None
        self._kafka_kwargs = kafka_kwargs
        self._running = False

    # ------------------------------------------------------------------
    # BaseConsumer interface
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=self._bootstrap,
            group_id=self._group_id,
            enable_auto_commit=False,   # manual commit after dedup
            auto_offset_reset="earliest",
            **self._kafka_kwargs,
        )
        await self._consumer.start()

        if self._dlq_topic:
            self._dlq_producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap)
            await self._dlq_producer.start()

        await self._store.connect() if hasattr(self._store, "connect") else None  # type: ignore[attr-defined]
        self._running = True
        logger.info(
            "DedupConsumer started topics=%s group=%s", self._topics, self._group_id
        )

    async def stop(self) -> None:
        self._running = False
        if self._consumer:
            await self._consumer.stop()
        if self._dlq_producer:
            await self._dlq_producer.stop()
        await self._store.close()
        logger.info("DedupConsumer stopped")

    async def run(self) -> None:
        if not self._consumer:
            raise RuntimeError("Call start() before run()")

        async for record in self._consumer:
            if not self._running:
                break
            await self._process_record(record)

    async def handle(self, message: Any) -> Any:  # noqa: ANN401
        """
        Override in subclasses to implement domain logic.

        Default implementation is a no-op that logs and returns None.
        """
        logger.info("DedupConsumer.handle (no-op): %s", message)
        return None

    # ------------------------------------------------------------------
    # Internal dedup lifecycle
    # ------------------------------------------------------------------

    async def _process_record(self, record: ConsumerRecord) -> None:
        message_id = _extract_message_id(record)

        if message_id is None:
            logger.warning(
                "No message_id found — processing without dedup "
                "topic=%s partition=%d offset=%d",
                record.topic, record.partition, record.offset,
            )
            self._metrics.record_no_message_id(record.topic)
            await self._execute_and_commit(record, message_id=None)
            return

        # Step 3: duplicate check
        with self._metrics.measure_store_op("is_duplicate"):
            is_dup = await self._store.is_duplicate(message_id)
        if is_dup:
            with self._metrics.measure_store_op("get_status"):
                existing = await self._store.get_status(message_id)
            # Allow retry if previous attempt failed and retry_failed is on
            if existing and existing.is_failed() and self._config.retry_failed:
                logger.info("Retrying failed message message_id=%s", message_id)
            else:
                logger.info("Skipping duplicate message_id=%s", message_id)
                self._metrics.record_duplicate(record.topic)
                await self._commit(record)
                return

        # Step 4: atomic claim
        with self._metrics.measure_store_op("claim"):
            claimed = await self._store.claim(message_id)
        if not claimed:
            logger.info("Lost claim race — skipping message_id=%s", message_id)
            self._metrics.record_claim_lost(record.topic)
            await self._commit(record)
            return

        with self._metrics.measure_processing(record.topic):
            await self._execute_and_commit(record, message_id=message_id)

    async def _execute_and_commit(
        self, record: ConsumerRecord, message_id: str | None
    ) -> None:
        """Run handle(), update dedup state, and commit offset."""
        import json

        try:
            payload = json.loads(record.value) if record.value else {}
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload = {"raw": record.value}

        attempts = 1
        last_exc: Exception | None = None

        while attempts <= self._config.max_retries:
            try:
                result = await self.handle(payload)
                # Step 6: mark completed and commit
                if message_id:
                    with self._metrics.measure_store_op("mark_completed"):
                        await self._store.mark_completed(message_id, result=result)
                await self._commit(record)
                self._metrics.record_processed(record.topic)
                logger.info(
                    "Message processed message_id=%s topic=%s offset=%d",
                    message_id, record.topic, record.offset,
                )
                return

            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                logger.warning(
                    "handle() failed attempt=%d/%d message_id=%s error=%s",
                    attempts, self._config.max_retries, message_id, exc,
                )
                self._metrics.record_error(record.topic, type(exc).__name__)
                if message_id:
                    with self._metrics.measure_store_op("mark_failed"):
                        await self._store.mark_failed(
                            message_id, error=str(exc), attempts=attempts
                        )
                if attempts < self._config.max_retries:
                    backoff = self._config.retry_backoff_ms / 1000 * attempts
                    await asyncio.sleep(backoff)
                    self._metrics.record_retry(record.topic)
                    if message_id:
                        with self._metrics.measure_store_op("claim"):
                            reclaimed = await self._store.claim(message_id)
                        if not reclaimed:
                            break
                attempts += 1

        # Exhausted retries → DLQ
        logger.error(
            "Message exhausted retries, sending to DLQ message_id=%s", message_id
        )
        self._metrics.record_dlq(record.topic)
        await self._send_to_dlq(record, error=str(last_exc))
        await self._commit(record)

    async def _commit(self, record: ConsumerRecord) -> None:
        if self._consumer:
            await self._consumer.commit()

    async def _send_to_dlq(self, record: ConsumerRecord, error: str) -> None:
        if not self._dlq_producer or not self._dlq_topic:
            logger.warning(
                "No DLQ configured — dropping failed message topic=%s offset=%d",
                record.topic, record.offset,
            )
            return
        import json
        dlq_payload = json.dumps({
            "original_topic": record.topic,
            "original_offset": record.offset,
            "original_partition": record.partition,
            "original_value": record.value.decode(errors="replace") if record.value else None,
            "error": error,
        }).encode()
        await self._dlq_producer.send_and_wait(
            self._dlq_topic,
            value=dlq_payload,
            key=record.key,
        )
        logger.info("Sent to DLQ topic=%s", self._dlq_topic)
