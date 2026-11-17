import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from aio_pika.abc import AbstractRobustConnection

from nexus.application.interfaces.event_publisher import EventPublisher
from nexus.infrastructure.config import EventBus, Settings

if TYPE_CHECKING:
    from vercel.queue import DeploymentOption, QueueClient

logger = logging.getLogger("nexus.messaging")


@dataclass
class PublisherHandle:
    publisher: EventPublisher | None
    close: Callable[[], Awaitable[None]]
    rabbitmq_connection: AbstractRobustConnection | None = None


async def _noop() -> None:
    return None


def vercel_queue_deployment(settings: Settings) -> "DeploymentOption":
    from vercel.queue import ALL_DEPLOYMENTS, CURRENT_DEPLOYMENT

    choice = settings.vercel_queue_deployment.strip()
    if choice == "all":
        return ALL_DEPLOYMENTS
    if choice == "current":
        return CURRENT_DEPLOYMENT
    if choice == "auto":
        if settings.vercel_deployment_id:
            return settings.vercel_deployment_id
        logger.warning(
            "VERCEL_DEPLOYMENT_ID is not set; publishing/polling Vercel Queues unpartitioned"
        )
        return ALL_DEPLOYMENTS
    return choice


def create_vercel_queue_client(settings: Settings) -> "QueueClient":
    from vercel.queue import QueueClient

    return QueueClient(
        region=settings.vercel_queue_region,
        timeout=settings.vercel_queue_publish_timeout_seconds,
        deployment=vercel_queue_deployment(settings),
    )


async def create_event_publisher(settings: Settings) -> PublisherHandle:
    """Builds the EventPublisher selected by EVENT_BUS. Imports are local so a
    deployment only loads (and needs credentials for) the bus it uses."""
    if settings.event_bus is EventBus.NONE:
        return PublisherHandle(publisher=None, close=_noop)

    if settings.event_bus is EventBus.VERCEL_QUEUES:
        from nexus.infrastructure.messaging.vercel_queues import VercelQueuesEventPublisher

        client = create_vercel_queue_client(settings)
        # QueueClient holds no open connections; nothing to close.
        return PublisherHandle(
            publisher=VercelQueuesEventPublisher(
                client,
                dispatch_topic=settings.vercel_queue_dispatch_topic,
                events_topic=settings.vercel_queue_events_topic,
            ),
            close=_noop,
        )

    from nexus.infrastructure.messaging.rabbitmq import create_rabbitmq_connection
    from nexus.infrastructure.messaging.rabbitmq_publisher import RabbitMQEventPublisher

    connection = await create_rabbitmq_connection(settings.rabbitmq_url)
    return PublisherHandle(
        publisher=RabbitMQEventPublisher(connection),
        close=connection.close,
        rabbitmq_connection=connection,
    )
