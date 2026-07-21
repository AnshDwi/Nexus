import json
from typing import Any

import aio_pika
from aio_pika.abc import AbstractRobustConnection

from nexus.domain.events import DomainEvent

_EXCHANGE_NAME = "nexus.events"


class RabbitMQEventPublisher:
    def __init__(self, connection: AbstractRobustConnection) -> None:
        self._connection = connection

    async def publish(self, event: DomainEvent) -> None:
        channel = await self._connection.channel()
        try:
            exchange = await channel.declare_exchange(
                _EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC, durable=True
            )
            await exchange.publish(
                aio_pika.Message(
                    body=json.dumps(_serialize(event)).encode("utf-8"),
                    message_id=str(event.event_id),
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=f"{event.aggregate_type}.{event.event_type}",
            )
        finally:
            await channel.close()


def _serialize(event: DomainEvent) -> dict[str, Any]:
    return {
        "event_id": str(event.event_id),
        "aggregate_id": event.aggregate_id,
        "aggregate_type": event.aggregate_type,
        "event_type": event.event_type,
        "event_version": event.event_version,
        "occurred_at": event.occurred_at.isoformat(),
        "correlation_id": str(event.correlation_id),
        "causation_id": str(event.causation_id) if event.causation_id else None,
        "payload": event.payload,
        "metadata": event.metadata,
    }
