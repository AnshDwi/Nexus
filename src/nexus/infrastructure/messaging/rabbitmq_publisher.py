import asyncio
import json
from typing import Any

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractRobustConnection

from nexus.domain.events import DomainEvent
from nexus.infrastructure.messaging.rabbitmq_topology import (
    EVENTS_EXCHANGE,
    WORK_ROUTING_KEYS,
    declare_topology,
)

_EXCHANGE_NAME = EVENTS_EXCHANGE


class RabbitMQEventPublisher:
    """Publishes outbox events with publisher confirms.

    `publish()` returns only after the broker has confirmed the message
    (persisted to a durable queue). Work messages (`*.dispatch_requested`)
    are published `mandatory`, with returns raised as errors: if no queue
    is bound, the publish fails and the outbox retries it, instead of the
    broker silently dropping a message the relay would then mark dispatched.
    Notification events are not mandatory (they may have no subscribers).
    """

    def __init__(
        self, connection: AbstractRobustConnection, *, publish_timeout_seconds: float = 10.0
    ) -> None:
        self._connection = connection
        self._timeout = publish_timeout_seconds
        self._channel: AbstractChannel | None = None
        self._exchange: AbstractExchange | None = None
        self._lock = asyncio.Lock()

    async def _exchange_ready(self) -> AbstractExchange:
        async with self._lock:
            if self._channel is None or self._channel.is_closed or self._exchange is None:
                self._channel = await self._connection.channel(
                    publisher_confirms=True, on_return_raises=True
                )
                topology = await declare_topology(self._channel)
                self._exchange = topology.events
            return self._exchange

    async def publish(self, event: DomainEvent) -> None:
        exchange = await self._exchange_ready()
        routing_key = f"{event.aggregate_type}.{event.event_type}"
        priority = event.metadata.get("broker_priority")
        headers: dict[str, Any] = {"aggregate_type": event.aggregate_type}
        if "attempt" in event.payload:
            headers["attempt"] = int(event.payload["attempt"])
        try:
            await exchange.publish(
                aio_pika.Message(
                    body=json.dumps(_serialize(event)).encode("utf-8"),
                    message_id=str(event.event_id),
                    correlation_id=str(
                        event.metadata.get("correlation_id") or event.correlation_id
                    ),
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    priority=int(priority) if priority is not None else None,
                    headers=headers,
                ),
                routing_key=routing_key,
                mandatory=routing_key in WORK_ROUTING_KEYS,
                timeout=self._timeout,
            )
        except Exception:
            # Force a fresh channel next time; a failed confirm can leave the
            # channel unusable.
            async with self._lock:
                self._channel, self._exchange = None, None
            raise

    async def close(self) -> None:
        if self._channel is not None and not self._channel.is_closed:
            await self._channel.close()


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
