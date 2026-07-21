from typing import Protocol

from nexus.domain.events import DomainEvent


class OutboxWriter(Protocol):
    """Producer-side port. The only outbox capability business use cases see."""

    async def add(self, event: DomainEvent) -> None: ...
