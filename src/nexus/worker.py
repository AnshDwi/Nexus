import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.application.interfaces.event_publisher import EventPublisher
from nexus.application.use_cases.relay_outbox_once import relay_outbox_once
from nexus.infrastructure.config import Settings, get_settings
from nexus.infrastructure.db.repositories.outbox_repository import PostgresOutboxRelayStore
from nexus.infrastructure.db.session import create_engine, create_session_factory
from nexus.infrastructure.messaging.factory import create_event_publisher
from nexus.infrastructure.observability.logging import configure_logging

logger = logging.getLogger("nexus.worker")


async def run_relay_iteration(
    session_factory: async_sessionmaker[AsyncSession],
    publisher: EventPublisher,
    settings: Settings,
) -> bool:
    """Run one relay tick inside its own transaction.

    A crash or exception anywhere in this call rolls back the whole
    transaction, leaving any claimed event unclaimed for a later retry.
    """
    async with session_factory() as session:
        store = PostgresOutboxRelayStore(
            session,
            max_attempts=settings.outbox_relay_max_attempts,
            initial_backoff_seconds=settings.outbox_relay_initial_backoff_seconds,
            max_backoff_seconds=settings.outbox_relay_max_backoff_seconds,
        )
        try:
            did_work = await relay_outbox_once(store, publisher)
        except Exception:
            await session.rollback()
            logger.exception("Outbox relay iteration failed")
            return False
        await session.commit()
        return did_work


async def run_relay_forever() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, settings)
    session_factory = create_session_factory(engine)
    # Honour EVENT_BUS so this relay and the scheduler daemon / eager relay
    # never publish the same outbox to two different brokers.
    handle = await create_event_publisher(settings)
    if handle.publisher is None:
        logger.warning("EVENT_BUS=none: outbox relay has nothing to publish to; exiting")
        await engine.dispose()
        return
    publisher = handle.publisher

    logger.info("Outbox relay starting", extra={"event_bus": settings.event_bus.value})
    try:
        while True:
            did_work = await run_relay_iteration(session_factory, publisher, settings)
            if not did_work:
                await asyncio.sleep(settings.outbox_relay_poll_interval_seconds)
    finally:
        await handle.close()
        await engine.dispose()


def main() -> None:
    configure_logging()
    asyncio.run(run_relay_forever())


if __name__ == "__main__":
    main()
