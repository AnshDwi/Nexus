import pytest
from sqlalchemy import delete

from nexus.infrastructure.config import get_settings
from nexus.infrastructure.db.models.leader_lease import LeaderLeaseModel
from nexus.infrastructure.db.models.outbox import OutboxRecordModel
from nexus.infrastructure.db.models.worker_registration import WorkerRegistrationModel
from nexus.infrastructure.db.session import create_engine, create_session_factory


@pytest.fixture
async def outbox_session_factory():
    engine = create_engine(get_settings().database_url)
    factory = create_session_factory(engine)
    yield factory
    async with factory() as session:
        await session.execute(delete(OutboxRecordModel))
        await session.commit()
    await engine.dispose()


@pytest.fixture
async def leader_lease_session_factory():
    engine = create_engine(get_settings().database_url)
    factory = create_session_factory(engine)
    yield factory
    async with factory() as session:
        await session.execute(delete(LeaderLeaseModel))
        await session.commit()
    await engine.dispose()


@pytest.fixture
async def worker_registry_session_factory():
    engine = create_engine(get_settings().database_url)
    factory = create_session_factory(engine)
    yield factory
    async with factory() as session:
        await session.execute(delete(WorkerRegistrationModel))
        await session.commit()
    await engine.dispose()


@pytest.fixture
async def task_session_factory():
    """Clean tasks/outbox/lease tables before and after each test."""
    from sqlalchemy import text

    engine = create_engine(get_settings().database_url)
    factory = create_session_factory(engine)

    async def _truncate() -> None:
        async with factory() as session:
            await session.execute(
                text(
                    "TRUNCATE task_transitions, tasks, outbox_records, scheduler_leader_lease "
                    "RESTART IDENTITY CASCADE"
                )
            )
            await session.commit()

    await _truncate()
    yield factory
    await _truncate()
    await engine.dispose()
