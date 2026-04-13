"""Abstract interface for the deduplication store."""

from abc import ABC, abstractmethod
from typing import Any

from .models import ProcessingStatus


class DeduplicationStore(ABC):
    """
    Defines the contract every dedup backend must satisfy.

    Atomic guarantee:
        claim() must be atomic — only ONE caller wins the race for a given
        message_id.  Backends implement this with Redis SET NX, database
        INSERT ... ON CONFLICT, etc.
    """

    @abstractmethod
    async def claim(self, message_id: str) -> bool:
        """
        Atomically claim the message_id for processing.

        Returns:
            True  — claim succeeded; this caller is the sole processor.
            False — already claimed by a previous (or concurrent) caller.
        """

    @abstractmethod
    async def is_duplicate(self, message_id: str) -> bool:
        """Return True if the message_id has already been claimed."""

    @abstractmethod
    async def mark_completed(self, message_id: str, result: Any = None) -> None:
        """Transition status to COMPLETED and optionally store the result."""

    @abstractmethod
    async def mark_failed(self, message_id: str, error: str, attempts: int = 1) -> None:
        """Transition status to FAILED and record the error."""

    @abstractmethod
    async def get_status(self, message_id: str) -> ProcessingStatus | None:
        """
        Return the current ProcessingStatus, or None if no record exists
        (i.e. the message has never been seen).
        """

    @abstractmethod
    async def delete(self, message_id: str) -> None:
        """Remove the record — used in tests and manual remediation."""

    @abstractmethod
    async def close(self) -> None:
        """Release any held resources (connections, pools, etc.)."""
