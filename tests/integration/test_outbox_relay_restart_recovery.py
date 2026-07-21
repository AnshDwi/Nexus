import pytest

from nexus.domain.events import DomainEvent
from nexus.infrastructure.config import get_settings
from nexus.infrastructure.db.repositories.outbox_repository import (
    PostgresOutboxRelayStore,
    PostgresOutboxWriter,
)
from nexus.worker import run_relay_iteration

pytestmark = pytest.mark.integration


class _FakePublisher:
    def __init__(self) -> None:
        self.published: list[DomainEvent] = []

    async def publish(self, event: DomainEvent) -> None:
        self.published.append(event)


def _event() -> DomainEvent:
    return DomainEvent(
        aggregate_id="order-1",
        aggregate_type="Order",
        event_type="OrderCreated",
        payload={"total": 42},
    )


async def test_crash_between_publish_and_commit_leaves_event_retryable(
    outbox_session_factory, monkeypatch
):
    """Simulates the 'crash after publish, before mark_dispatched commits' scenario.

    The event is genuinely published once, then the process 'dies' before
    recording success. This must roll back cleanly and leave the event
    claimable again -- the redelivery this causes is expected and is exactly
    why consumers must be idempotent (see M1's Inbox pattern).
    """
    event = _event()
    async with outbox_session_factory() as session:
        await PostgresOutboxWriter(session).add(event)
        await session.commit()

    settings = get_settings()
    publisher = _FakePublisher()

    async def _simulated_crash(self, event_id):
        raise ConnectionError("simulated crash before commit")

    monkeypatch.setattr(PostgresOutboxRelayStore, "mark_dispatched", _simulated_crash)

    did_work = await run_relay_iteration(outbox_session_factory, publisher, settings)

    assert did_work is False
    assert publisher.published == [event]

    monkeypatch.undo()

    did_work_after_recovery = await run_relay_iteration(outbox_session_factory, publisher, settings)

    assert did_work_after_recovery is True
    assert len(publisher.published) == 2
    assert publisher.published[1].event_id == event.event_id
