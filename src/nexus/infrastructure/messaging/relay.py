import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.application.interfaces.event_publisher import EventPublisher
from nexus.infrastructure.config import Settings
from nexus.worker import run_relay_iteration

logger = logging.getLogger("nexus.outbox.relay")


async def relay_pending_events(
    session_factory: async_sessionmaker[AsyncSession],
    publisher: EventPublisher | None,
    settings: Settings,
    *,
    max_events: int,
) -> int:
    """Relays up to `max_events` outbox rows, one transaction each (the
    existing relay iteration: SKIP LOCKED claim, publish, mark/back off).

    Used three ways: eagerly by a request right after it commits (so a task
    is dispatched within the same invocation), by the cron sweep, and by the
    daemon loop. All three can run concurrently; SKIP LOCKED keeps them off
    each other's rows and the publish idempotency key absorbs the
    crash-after-publish window. Never raises: a failure leaves rows for the
    next relay.
    """
    if publisher is None:
        return 0
    relayed = 0
    for _ in range(max_events):
        try:
            did_work = await run_relay_iteration(session_factory, publisher, settings)
        except Exception:
            logger.exception("outbox relay iteration crashed")
            break
        if not did_work:
            break
        relayed += 1
    return relayed
