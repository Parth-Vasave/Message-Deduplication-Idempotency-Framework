"""
Prometheus metrics for the Kafka deduplication framework.

Usage::

    from src.metrics.dedup_metrics import metrics

    metrics.record_processed("orders")
    metrics.record_duplicate("orders")

    with metrics.measure_processing("orders"):
        result = await handle(payload)

    with metrics.measure_store_op("claim"):
        claimed = await store.claim(message_id)
"""

import time
from collections.abc import Generator
from contextlib import contextmanager

from prometheus_client import Counter, Gauge, Histogram, make_asgi_app

# ---------------------------------------------------------------------------
# Metric definitions  (module-level singletons — registered once at import)
# ---------------------------------------------------------------------------

messages_total = Counter(
    "dedup_messages_total",
    "Total messages received by the dedup consumer, labelled by outcome",
    ["topic", "status"],
    # status values: processed | duplicate | claim_lost | no_id | dlq
)

processing_duration_seconds = Histogram(
    "dedup_processing_duration_seconds",
    "End-to-end wall-clock time to process one message (from claim to commit)",
    ["topic"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

store_operation_duration_seconds = Histogram(
    "dedup_store_operation_duration_seconds",
    "Latency of individual DeduplicationStore operations (Redis calls)",
    ["operation"],
    # operation values: claim | is_duplicate | mark_completed | mark_failed | get_status
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

retry_attempts_total = Counter(
    "dedup_retry_attempts_total",
    "Number of retry attempts for failed messages (excludes the first attempt)",
    ["topic"],
)

dlq_messages_total = Counter(
    "dedup_dlq_messages_total",
    "Messages routed to the Dead Letter Queue after exhausting retries",
    ["topic"],
)

active_processing = Gauge(
    "dedup_active_processing",
    "Number of messages currently being processed (between claim and commit)",
    ["topic"],
)

consumer_errors_total = Counter(
    "dedup_consumer_errors_total",
    "Unhandled errors in the dedup consumer loop",
    ["topic", "error_type"],
)


# ---------------------------------------------------------------------------
# Helper class
# ---------------------------------------------------------------------------


class DeduplicationMetrics:
    """
    Thin wrapper around the module-level Prometheus objects.

    Keeps recording calls readable at the call site and centralises the label
    naming so changing a metric name only requires a single edit here.
    """

    # --- event counters -------------------------------------------------------

    def record_processed(self, topic: str) -> None:
        """Message completed successfully."""
        messages_total.labels(topic=topic, status="processed").inc()

    def record_duplicate(self, topic: str) -> None:
        """Message was detected as a duplicate and skipped."""
        messages_total.labels(topic=topic, status="duplicate").inc()

    def record_claim_lost(self, topic: str) -> None:
        """Lost the atomic SET NX race — another consumer claimed first."""
        messages_total.labels(topic=topic, status="claim_lost").inc()

    def record_no_message_id(self, topic: str) -> None:
        """Message arrived without an extractable message_id."""
        messages_total.labels(topic=topic, status="no_id").inc()

    def record_dlq(self, topic: str) -> None:
        """Message was sent to the Dead Letter Queue."""
        messages_total.labels(topic=topic, status="dlq").inc()
        dlq_messages_total.labels(topic=topic).inc()

    def record_retry(self, topic: str) -> None:
        """One retry attempt was made."""
        retry_attempts_total.labels(topic=topic).inc()

    def record_error(self, topic: str, error_type: str) -> None:
        """An unexpected error surfaced in the consumer loop."""
        consumer_errors_total.labels(topic=topic, error_type=error_type).inc()

    # --- timing context managers ---------------------------------------------

    @contextmanager
    def measure_processing(self, topic: str) -> Generator[None, None, None]:
        """
        Track active count and total processing duration for one message.

        Usage::

            with metrics.measure_processing(record.topic):
                await self._execute_and_commit(record, message_id)
        """
        active_processing.labels(topic=topic).inc()
        start = time.perf_counter()
        try:
            yield
        finally:
            active_processing.labels(topic=topic).dec()
            processing_duration_seconds.labels(topic=topic).observe(time.perf_counter() - start)

    @contextmanager
    def measure_store_op(self, operation: str) -> Generator[None, None, None]:
        """
        Time a single DeduplicationStore operation.

        Usage::

            with metrics.measure_store_op("claim"):
                claimed = await self._store.claim(message_id)
        """
        start = time.perf_counter()
        try:
            yield
        finally:
            store_operation_duration_seconds.labels(operation=operation).observe(
                time.perf_counter() - start
            )


# ---------------------------------------------------------------------------
# Global singleton — import this in consumers / stores
# ---------------------------------------------------------------------------

metrics = DeduplicationMetrics()

# ---------------------------------------------------------------------------
# ASGI app for /metrics endpoint (mount in FastAPI or run standalone)
# ---------------------------------------------------------------------------

metrics_asgi_app = make_asgi_app()
