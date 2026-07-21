from typing import Protocol
from uuid import UUID

from nexus.domain.events import DomainEvent


class OutboxRelayStore(Protocol):
    """Relay-side port. Deliberately separate from OutboxWriter (ISP) so business
    use cases can never reach relay-management operations."""

    async def claim_next(self) -> DomainEvent | None:
        """Atomically claim the oldest pending event, if any."""
        ...

    async def mark_dispatched(self, event_id: UUID) -> None: ...

    async def record_failure(self, event_id: UUID, error: str) -> None:
        """Record a failed publish attempt; the store owns backoff/dead-letter policy."""
        ...
