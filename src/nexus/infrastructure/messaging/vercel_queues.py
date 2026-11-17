"""Vercel Queues adapter for the EventPublisher port.

The outbox relay calls `publish()` once per outbox row. Each send carries the
outbox `event_id` as the Vercel Queues idempotency key, so a relay that
publishes, crashes before marking the row dispatched, and publishes again is
deduplicated by the queue service itself (the dedup window is the message's
retention period). Consumers still get at-least-once delivery and must be
idempotent -- the task executor's claim guard provides that.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from vercel.queue import QueueClient

from nexus.application.use_cases.task_events import (
    DELIVER_AFTER_METADATA_KEY,
    DISPATCH_EVENT_TYPE,
    TASK_AGGREGATE,
)
from nexus.domain.events import DomainEvent

_MIN_RETENTION = timedelta(hours=24)
_MAX_RETENTION = timedelta(days=7)
_RETENTION_HEADROOM = timedelta(hours=1)
# Vercel Queues rejects delays longer than 7 days (and longer than retention).
_MAX_DELAY = _MAX_RETENTION - _RETENTION_HEADROOM


def is_dispatch_event(event: DomainEvent) -> bool:
    return event.aggregate_type == TASK_AGGREGATE and event.event_type == DISPATCH_EVENT_TYPE


def delivery_delay(event: DomainEvent, *, now: datetime) -> timedelta:
    raw = event.metadata.get(DELIVER_AFTER_METADATA_KEY)
    if not raw:
        return timedelta(0)
    deliver_after = datetime.fromisoformat(str(raw))
    if deliver_after.tzinfo is None:
        deliver_after = deliver_after.replace(tzinfo=UTC)
    return min(max(deliver_after - now, timedelta(0)), _MAX_DELAY)


def serialize_event(event: DomainEvent) -> dict[str, Any]:
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


class VercelQueuesEventPublisher:
    def __init__(
        self,
        client: QueueClient,
        *,
        dispatch_topic: str,
        events_topic: str,
        job_topic: str = "nexus-job-dispatch",
    ) -> None:
        self._client = client
        self._dispatch_topic = dispatch_topic
        self._events_topic = events_topic
        self._job_topic = job_topic

    async def publish(self, event: DomainEvent) -> None:
        now = datetime.now(UTC)
        delay = delivery_delay(event, now=now)
        retention = min(max(_MIN_RETENTION, delay + _RETENTION_HEADROOM), _MAX_RETENTION)
        if is_dispatch_event(event) or (
            event.aggregate_type == "job" and event.event_type == DISPATCH_EVENT_TYPE
        ):
            topic = self._dispatch_topic if is_dispatch_event(event) else self._job_topic
            payload: dict[str, Any] = {**event.payload, "event_id": str(event.event_id)}
        else:
            topic = self._events_topic
            payload = serialize_event(event)

        await self._client.send(
            topic,
            payload,
            idempotency_key=str(event.event_id),
            delay=delay.total_seconds() if delay > timedelta(0) else None,
            retention=retention.total_seconds(),
            headers={"x-correlation-id": str(event.correlation_id)},
        )
