"""
Abstract base consumer.

Subclass this (or use DedupConsumer directly) to get the full
start/stop/run lifecycle without worrying about dedup concerns.
"""

from abc import ABC, abstractmethod
from typing import Any


class BaseConsumer(ABC):
    """
    Minimal async consumer contract.

    Lifecycle:
        await consumer.start()
        await consumer.run()       # blocks until stopped
        await consumer.stop()
    """

    @abstractmethod
    async def start(self) -> None:
        """Connect to Kafka and any auxiliary services (Redis, DB)."""

    @abstractmethod
    async def stop(self) -> None:
        """Gracefully shut down: drain in-flight messages, close connections."""

    @abstractmethod
    async def run(self) -> None:
        """Poll Kafka and dispatch messages until stop() is called."""

    @abstractmethod
    async def handle(self, message: Any) -> Any:
        """
        Process a single *deduplicated* message.

        Implement your domain logic here.  The return value is stored as the
        processing result in the dedup store.

        Raise any exception to trigger the failed/retry path.
        """
