from collections.abc import Awaitable, Callable

from nexus.application.interfaces.inbox_repository import InboxRepository
from nexus.domain.idempotency import IdempotencyKey


async def process_idempotently(
    inbox_repository: InboxRepository,
    key: IdempotencyKey,
    handler: Callable[[], Awaitable[None]],
) -> bool:
    """Run `handler` at most once per `key`.

    Returns True if `handler` ran (this call did the work), False if `key`
    was already processed and `handler` was skipped as a duplicate.
    """
    is_new = await inbox_repository.mark_processed_if_new(key)
    if not is_new:
        return False
    await handler()
    return True
