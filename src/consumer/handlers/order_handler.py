"""
OrderHandler — stub for order-creation business logic.

You own this file.  Implement `handle()` with your actual SQL / service calls.
The dedup lifecycle (claim → process → mark_completed/failed) is handled by
DedupConsumer; this class only needs to contain domain logic.
"""

import logging
from typing import Any

from src.consumer.dedup_consumer import DedupConsumer
from src.dedup.store import DeduplicationStore

logger = logging.getLogger(__name__)


class OrderHandler(DedupConsumer):
    """
    Consumes from the ``orders`` topic and creates orders.

    Override handle() with real SQL once you wire up asyncpg.
    """

    def __init__(self, store: DeduplicationStore, **kwargs: Any) -> None:
        super().__init__(
            topics=["orders"],
            store=store,
            dlq_topic="orders.dlq",
            **kwargs,
        )

    async def handle(self, message: dict[str, Any]) -> dict[str, Any]:
        """
        TODO: Replace with real order-creation logic, e.g.:
            async with db_pool.acquire() as conn:
                order_id = await conn.fetchval(
                    "INSERT INTO orders (...) VALUES (...) RETURNING id", ...
                )
        """
        order_id = message.get("order_id", "unknown")
        logger.info("Creating order order_id=%s", order_id)
        # Simulate work
        return {"order_id": order_id, "status": "created"}
