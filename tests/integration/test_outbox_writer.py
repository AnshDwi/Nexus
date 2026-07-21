import pytest

from nexus.domain.events import DomainEvent
from nexus.infrastructure.db.models.outbox import OutboxRecordModel
from nexus.infrastructure.db.repositories.outbox_repository import PostgresOutboxWriter

pytestmark = pytest.mark.integration


def _event() -> DomainEvent:
    return DomainEvent(
        aggregate_id="order-1",
        aggregate_type="Order",
        event_type="OrderCreated",
        payload={"total": 42},
    )


async def test_outbox_row_does_not_survive_a_rolled_back_transaction(outbox_session_factory):
    event = _event()

    async with outbox_session_factory() as session:
        await PostgresOutboxWriter(session).add(event)
        await session.rollback()

    async with outbox_session_factory() as session:
        record = await session.get(OutboxRecordModel, event.event_id)

    assert record is None


async def test_outbox_row_persists_after_commit(outbox_session_factory):
    event = _event()

    async with outbox_session_factory() as session:
        await PostgresOutboxWriter(session).add(event)
        await session.commit()

    async with outbox_session_factory() as session:
        record = await session.get(OutboxRecordModel, event.event_id)

    assert record is not None
    assert record.aggregate_id == event.aggregate_id
    assert record.payload == event.payload
    assert record.dispatched_at is None
