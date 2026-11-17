from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from nexus.application.use_cases.task_events import DELIVER_AFTER_METADATA_KEY
from nexus.domain.events import DomainEvent
from nexus.infrastructure.messaging.vercel_queues import (
    VercelQueuesEventPublisher,
    delivery_delay,
)


class _FakeClient:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    async def send(self, topic: str, payload: Any, **kwargs: Any) -> str:
        self.sent.append({"topic": topic, "payload": payload, **kwargs})
        return "msg_1"


def _dispatch_event(**metadata: Any) -> DomainEvent:
    task_id = uuid4()
    return DomainEvent(
        aggregate_id=str(task_id),
        aggregate_type="task",
        event_type="dispatch_requested",
        payload={"task_id": str(task_id), "attempt": 2},
        metadata=metadata,
    )


def _publisher(client: _FakeClient) -> VercelQueuesEventPublisher:
    return VercelQueuesEventPublisher(
        client,  # type: ignore[arg-type]
        dispatch_topic="nexus-task-dispatch",
        events_topic="nexus-events",
    )


async def test_dispatch_events_go_to_dispatch_topic_with_event_id_idempotency_key():
    client = _FakeClient()
    event = _dispatch_event()
    await _publisher(client).publish(event)

    (sent,) = client.sent
    assert sent["topic"] == "nexus-task-dispatch"
    assert sent["payload"] == {**event.payload, "event_id": str(event.event_id)}
    assert sent["idempotency_key"] == str(event.event_id)
    assert sent["delay"] is None
    assert sent["retention"] == timedelta(hours=24).total_seconds()


async def test_other_events_go_to_events_topic_fully_serialized():
    client = _FakeClient()
    event = DomainEvent(
        aggregate_id="t1", aggregate_type="task", event_type="succeeded", payload={"x": 1}
    )
    await _publisher(client).publish(event)

    (sent,) = client.sent
    assert sent["topic"] == "nexus-events"
    assert sent["payload"]["event_type"] == "succeeded"
    assert sent["payload"]["payload"] == {"x": 1}


async def test_deliver_after_becomes_a_queue_delay_with_enough_retention():
    client = _FakeClient()
    later = datetime.now(UTC) + timedelta(hours=30)
    await _publisher(client).publish(
        _dispatch_event(**{DELIVER_AFTER_METADATA_KEY: later.isoformat()})
    )

    (sent,) = client.sent
    assert 29.9 * 3600 < sent["delay"] <= 30 * 3600
    assert sent["retention"] >= sent["delay"] + 3600


def test_delay_is_clamped_to_queue_limits():
    now = datetime.now(UTC)
    past = _dispatch_event(**{DELIVER_AFTER_METADATA_KEY: (now - timedelta(hours=1)).isoformat()})
    far = _dispatch_event(**{DELIVER_AFTER_METADATA_KEY: (now + timedelta(days=30)).isoformat()})
    assert delivery_delay(past, now=now) == timedelta(0)
    assert delivery_delay(far, now=now) <= timedelta(days=7)
