import asyncio

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker

from nexus.domain.idempotency import IdempotencyKey
from nexus.infrastructure.config import get_settings
from nexus.infrastructure.db.models.inbox import InboxRecordModel
from nexus.infrastructure.db.repositories.inbox_repository import PostgresInboxRepository
from nexus.infrastructure.db.session import create_engine, create_session_factory

pytestmark = pytest.mark.integration


@pytest.fixture
async def session_factory():
    engine = create_engine(get_settings().database_url)
    factory = create_session_factory(engine)
    yield factory
    async with factory() as session:
        await session.execute(delete(InboxRecordModel))
        await session.commit()
    await engine.dispose()


async def _mark_processed(factory: async_sessionmaker, key: IdempotencyKey) -> bool:
    async with factory() as session:
        result = await PostgresInboxRepository(session).mark_processed_if_new(key)
        await session.commit()
        return result


async def test_mark_processed_if_new_is_true_only_once_for_same_key(session_factory):
    key = IdempotencyKey("race-key")

    first = await _mark_processed(session_factory, key)
    second = await _mark_processed(session_factory, key)

    assert first is True
    assert second is False


async def test_mark_processed_if_new_is_race_safe_under_concurrency(session_factory):
    key = IdempotencyKey("concurrent-key")

    results = await asyncio.gather(*(_mark_processed(session_factory, key) for _ in range(10)))

    assert results.count(True) == 1
    assert results.count(False) == 9
