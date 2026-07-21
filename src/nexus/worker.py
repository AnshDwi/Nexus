import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.application.interfaces.event_publisher import EventPublisher
from nexus.application.use_cases.relay_outbox_once import relay_outbox_once
from nexus.infrastructure.config import Settings, get_settings
from nexus.infrastructure.db.repositories.outbox_repository import PostgresOutboxRelayStore
from nexus.infrastructure.db.session import create_engine, create_session_factory
from nexus.infrastructure.messaging.rabbitmq import create_rabbitmq_connection
from nexus.infrastructure.messaging.rabbitmq_publisher import RabbitMQEventPublisher

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
    engine = create_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    rabbitmq_connection = await create_rabbitmq_connection(settings.rabbitmq_url)
    publisher = RabbitMQEventPublisher(rabbitmq_connection)

    logger.info("Outbox relay starting")
    try:
        while True:
            did_work = await run_relay_iteration(session_factory, publisher, settings)
            if not did_work:
                await asyncio.sleep(settings.outbox_relay_poll_interval_seconds)
    finally:
        await rabbitmq_connection.close()
        await engine.dispose()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_relay_forever())


if __name__ == "__main__":
    main()
