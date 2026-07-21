import asyncio

import pytest

from nexus.domain.events import DomainEvent
from nexus.infrastructure.db.repositories.outbox_repository import (
    PostgresOutboxRelayStore,
    PostgresOutboxWriter,
)

pytestmark = pytest.mark.integration


def _event(**overrides) -> DomainEvent:
    defaults = {
        "aggregate_id": "order-1",
        "aggregate_type": "Order",
        "event_type": "OrderCreated",
        "payload": {"total": 42},
    }
    defaults.update(overrides)
    return DomainEvent(**defaults)


async def _seed(session_factory, event: DomainEvent) -> None:
    async with session_factory() as session:
        await PostgresOutboxWriter(session).add(event)
        await session.commit()


def _store(session, **overrides) -> PostgresOutboxRelayStore:
    defaults = {
        "max_attempts": 3,
        "initial_backoff_seconds": 1.0,
        "max_backoff_seconds": 60.0,
    }
    defaults.update(overrides)
    return PostgresOutboxRelayStore(session, **defaults)


async def test_claim_next_is_race_safe_under_concurrency(outbox_session_factory):
    event = _event()
    await _seed(outbox_session_factory, event)

    sessions = [outbox_session_factory() for _ in range(10)]
    for session in sessions:
        await session.__aenter__()

    try:
        results = await asyncio.gather(*(_store(session).claim_next() for session in sessions))
    finally:
        for session in sessions:
            await session.rollback()
            await session.__aexit__(None, None, None)

    claimed = [result for result in results if result is not None]
    assert len(claimed) == 1
    assert claimed[0].event_id == event.event_id


async def test_claim_next_ignores_already_dispatched_rows(outbox_session_factory):
    event = _event()
    await _seed(outbox_session_factory, event)

    async with outbox_session_factory() as session:
        store = _store(session)
        claimed = await store.claim_next()
        assert claimed is not None
        await store.mark_dispatched(claimed.event_id)
        await session.commit()

    async with outbox_session_factory() as session:
        second_claim = await _store(session).claim_next()

    assert second_claim is None


async def test_record_failure_defers_retry_until_backoff_elapses(outbox_session_factory):
    event = _event()
    await _seed(outbox_session_factory, event)

    async with outbox_session_factory() as session:
        store = _store(session, initial_backoff_seconds=3600.0)
        claimed = await store.claim_next()
        assert claimed is not None
        await store.record_failure(claimed.event_id, "boom")
        await session.commit()

    async with outbox_session_factory() as session:
        immediate_retry = await _store(session).claim_next()

    assert immediate_retry is None


async def test_record_failure_dead_letters_after_max_attempts(outbox_session_factory):
    event = _event()
    await _seed(outbox_session_factory, event)

    for _ in range(3):
        async with outbox_session_factory() as session:
            store = _store(session, max_attempts=3, initial_backoff_seconds=0.0)
            claimed = await store.claim_next()
            assert claimed is not None
            await store.record_failure(claimed.event_id, "boom")
            await session.commit()

    async with outbox_session_factory() as session:
        after_dead_letter = await _store(session).claim_next()

    assert after_dead_letter is None
