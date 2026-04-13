"""
ShipmentHandler — stub for shipment-dispatch business logic.

You own this file.  Wire in your shipping carrier API (EasyPost, ShipStation,
etc.) inside handle().
"""

import logging
from typing import Any

from src.consumer.dedup_consumer import DedupConsumer
from src.dedup.store import DeduplicationStore

logger = logging.getLogger(__name__)


class ShipmentHandler(DedupConsumer):
    """
    Consumes from the ``shipments`` topic and dispatches shipments.
    """

    def __init__(self, store: DeduplicationStore, **kwargs: Any) -> None:
        super().__init__(
            topics=["shipments"],
            store=store,
            dlq_topic="shipments.dlq",
            **kwargs,
        )

    async def handle(self, message: dict[str, Any]) -> dict[str, Any]:
        """
        TODO: Dispatch shipment via carrier API.
        """
        shipment_id = message.get("shipment_id", "unknown")
        address = message.get("address", {})
        logger.info("Dispatching shipment shipment_id=%s", shipment_id)
        # Simulate work
        return {"shipment_id": shipment_id, "tracking_number": "TRACK-STUB", "address": address}
