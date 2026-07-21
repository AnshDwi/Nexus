import asyncio
import json

import aio_pika
import pytest

from nexus.domain.events import DomainEvent
from nexus.infrastructure.config import get_settings
from nexus.infrastructure.messaging.rabbitmq import create_rabbitmq_connection
from nexus.infrastructure.messaging.rabbitmq_publisher import RabbitMQEventPublisher

pytestmark = pytest.mark.integration


def _event() -> DomainEvent:
    return DomainEvent(
        aggregate_id="order-1",
        aggregate_type="Order",
        event_type="OrderCreated",
        payload={"total": 42},
    )


async def _consume_one(queue: aio_pika.abc.AbstractQueue, timeout: float = 5.0) -> bytes:
    async def _get() -> bytes:
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    return message.body
        raise AssertionError("queue iterator ended without yielding a message")

    return await asyncio.wait_for(_get(), timeout=timeout)


async def test_publish_delivers_event_to_a_bound_queue():
    settings = get_settings()
    connection = await create_rabbitmq_connection(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            "nexus.events", aio_pika.ExchangeType.TOPIC, durable=True
        )
        queue = await channel.declare_queue(exclusive=True)
        await queue.bind(exchange, routing_key="Order.OrderCreated")

        event = _event()
        await RabbitMQEventPublisher(connection).publish(event)

        body = json.loads(await _consume_one(queue))

        assert body["event_id"] == str(event.event_id)
        assert body["aggregate_type"] == "Order"
        assert body["event_type"] == "OrderCreated"
        assert body["payload"] == {"total": 42}
    finally:
        await connection.close()
