from uuid import UUID

from nexus.application.use_cases.relay_outbox_once import relay_outbox_once
from nexus.domain.events import DomainEvent


class _FakeRelayStore:
    def __init__(self, events: list[DomainEvent]) -> None:
        self._queue = list(events)
        self.dispatched: list[UUID] = []
        self.failures: list[tuple[UUID, str]] = []

    async def claim_next(self) -> DomainEvent | None:
        if not self._queue:
            return None
        return self._queue.pop(0)

    async def mark_dispatched(self, event_id: UUID) -> None:
        self.dispatched.append(event_id)

    async def record_failure(self, event_id: UUID, error: str) -> None:
        self.failures.append((event_id, error))


class _FakePublisher:
    def __init__(self, *, should_fail: bool = False) -> None:
        self._should_fail = should_fail
        self.published: list[DomainEvent] = []

    async def publish(self, event: DomainEvent) -> None:
        if self._should_fail:
            raise ConnectionError("broker unreachable")
        self.published.append(event)


def _event() -> DomainEvent:
    return DomainEvent(
        aggregate_id="order-1",
        aggregate_type="Order",
        event_type="OrderCreated",
        payload={"total": 42},
    )


async def test_relay_outbox_once_returns_false_when_queue_is_empty():
    store = _FakeRelayStore([])
    publisher = _FakePublisher()

    did_work = await relay_outbox_once(store, publisher)

    assert did_work is False
    assert publisher.published == []


async def test_relay_outbox_once_marks_dispatched_on_success():
    event = _event()
    store = _FakeRelayStore([event])
    publisher = _FakePublisher()

    did_work = await relay_outbox_once(store, publisher)

    assert did_work is True
    assert publisher.published == [event]
    assert store.dispatched == [event.event_id]
    assert store.failures == []


async def test_relay_outbox_once_records_failure_and_does_not_mark_dispatched():
    event = _event()
    store = _FakeRelayStore([event])
    publisher = _FakePublisher(should_fail=True)

    did_work = await relay_outbox_once(store, publisher)

    assert did_work is True
    assert store.dispatched == []
    assert len(store.failures) == 1
    assert store.failures[0][0] == event.event_id
    assert "broker unreachable" in store.failures[0][1]
