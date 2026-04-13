"""
PaymentHandler — stub for payment-processing business logic.

You own this file.  Wire in your payment processor SDK (Stripe, Braintree, etc.)
inside handle() and add the outbox pattern to publish payment-processed events.
"""

import logging
from typing import Any

from src.consumer.dedup_consumer import DedupConsumer
from src.dedup.store import DeduplicationStore

logger = logging.getLogger(__name__)


class PaymentHandler(DedupConsumer):
    """
    Consumes from the ``payments`` topic and charges customers.
    """

    def __init__(self, store: DeduplicationStore, **kwargs: Any) -> None:
        super().__init__(
            topics=["payments"],
            store=store,
            dlq_topic="payments.dlq",
            **kwargs,
        )

    async def handle(self, message: dict[str, Any]) -> dict[str, Any]:
        """
        TODO: Call your payment processor here.
        The idempotency_key for the payment API should be message["message_id"]
        so that duplicate retries to the payment provider are also safe.
        """
        payment_id = message.get("payment_id", "unknown")
        amount = message.get("amount", 0)
        logger.info("Processing payment payment_id=%s amount=%s", payment_id, amount)
        # Simulate work
        return {"payment_id": payment_id, "charged": amount, "status": "success"}
