from typing import Protocol

from nexus.domain.idempotency import IdempotencyKey


class InboxRepository(Protocol):
    async def mark_processed_if_new(self, key: IdempotencyKey) -> bool:
        """Atomically record `key` as processed.

        Returns True if this call recorded a new key (caller should proceed),
        False if the key was already recorded (caller should skip as a duplicate).
        """
        ...
