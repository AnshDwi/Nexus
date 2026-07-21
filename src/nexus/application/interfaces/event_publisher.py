from typing import Protocol

from nexus.domain.events import DomainEvent


class EventPublisher(Protocol):
    async def publish(self, event: DomainEvent) -> None: ...
