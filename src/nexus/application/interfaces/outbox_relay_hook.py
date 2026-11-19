from typing import Protocol

from nexus.domain.events import DomainEvent


class OutboxRelayHook(Protocol):
    """Lets an aggregate react, inside the relay's transaction, to its event
    being published -- e.g. a job becomes QUEUED only in the transaction that
    observed the broker's confirmation."""

    async def before_publish(self, event: DomainEvent) -> bool:
        """Return False to skip publishing (the event is obsolete); the row
        is then marked dispatched without reaching the broker."""
        ...

    async def after_publish(self, event: DomainEvent) -> None: ...
