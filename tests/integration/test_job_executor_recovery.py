"""JobExecutor hand-back paths against real PostgreSQL (no broker needed):

* G4 stranded claim: a claim committed although the worker saw an error;
  the redelivery is an inbox duplicate, and must hand the attempt back
  instead of leaving it RUNNING until its lease expires;
* authority lost while the claim was in flight: the handler never starts;
* a handler that calls TaskContext.check_authority() after the worker
  fenced: the attempt is handed back, not reported.
"""

import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from nexus.application.interfaces.task_handler import TaskContext
from nexus.application.use_cases.jobs import claim_job
from nexus.domain.job import Job, JobPriority, JobState
from nexus.infrastructure.config import get_settings
from nexus.infrastructure.coordination.execution_authority import ExecutionAuthority
from nexus.infrastructure.db.repositories.inbox_repository import PostgresInboxRepository
from nexus.infrastructure.db.repositories.job_repository import PostgresJobStore
from nexus.infrastructure.db.session import create_engine, create_session_factory
from nexus.infrastructure.execution.handlers import InMemoryTaskHandlerRegistry
from nexus.infrastructure.execution.job_executor import Disposition, JobExecutor, JobMessage

pytestmark = pytest.mark.integration


@dataclass
class Env:
    sf: Any
    calls: list[int]

    def executor(self, handler: Any, *, session_factory: Any = None) -> JobExecutor:
        return JobExecutor(
            session_factory or self.sf,
            InMemoryTaskHandlerRegistry({"probe": handler}),
            worker_id=uuid4(),
            lease_grace=timedelta(seconds=30),
        )

    async def queued_job(self) -> UUID:
        now = datetime.now(UTC)
        job = Job(
            job_id=uuid4(),
            tenant_id="acme",
            name="recovery",
            job_type="probe",
            payload={},
            priority=JobPriority.NORMAL,
            state=JobState.QUEUED,
            attempt=1,
            max_attempts=3,
            retry_limit=3,
            initial_backoff_seconds=0,
            max_backoff_seconds=0,
            timeout_seconds=60,
            created_at=now,
            updated_at=now,
        )
        async with self.sf() as session:
            await PostgresJobStore(session).insert_or_get_existing(job)
            await session.commit()
        return job.job_id

    async def claim_as(self, job_id: UUID, worker_id: UUID, message_id: str) -> None:
        """A claim that committed, as if the worker never heard back."""
        async with self.sf() as session:
            await claim_job(
                PostgresJobStore(session),
                PostgresInboxRepository(session),
                job_id=job_id,
                attempt=1,
                message_id=message_id,
                worker_id=worker_id,
                lease_grace=timedelta(seconds=30),
            )
            await session.commit()

    async def job(self, job_id: UUID) -> tuple[str, int]:
        async with self.sf() as session:
            row = (
                await session.execute(
                    text("SELECT state, attempt FROM jobs WHERE job_id = :j"), {"j": job_id}
                )
            ).one()
        return row[0], row[1]

    async def executions(self, job_id: UUID) -> list[tuple[int, str, str | None]]:
        async with self.sf() as session:
            rows = await session.execute(
                text(
                    "SELECT attempt, status, error FROM job_executions WHERE job_id = :j "
                    "ORDER BY id"
                ),
                {"j": job_id},
            )
            return [(r[0], r[1], r[2]) for r in rows]


@pytest.fixture
async def env() -> AsyncIterator[Env]:
    get_settings.cache_clear()
    settings = get_settings()
    engine = create_engine(settings.database_url, settings)
    sf = create_session_factory(engine)
    async with sf() as session:
        await session.execute(
            text(
                "TRUNCATE job_executions, job_transitions, jobs, inbox_records, outbox_records "
                "RESTART IDENTITY CASCADE"
            )
        )
        await session.commit()
    yield Env(sf, [])
    await engine.dispose()
    get_settings.cache_clear()


def _authorized() -> ExecutionAuthority:
    authority = ExecutionAuthority(fence_after=60)
    authority.confirm(time.monotonic())
    return authority


async def test_stranded_claim_is_handed_back_on_redelivery(env: Env):
    async def handler(payload: dict[str, Any], context: TaskContext) -> Any:
        env.calls.append(context.attempt)
        return {}

    executor = env.executor(handler)
    job_id = await env.queued_job()
    await env.claim_as(job_id, executor.worker_id, "msg-1")  # committed; worker saw an error
    assert await env.job(job_id) == ("running", 1)

    disposition = await executor.handle(JobMessage(job_id=job_id, attempt=1, message_id="msg-1"))
    assert disposition is Disposition.ACK
    assert env.calls == []  # nothing ran twice
    assert await env.job(job_id) == ("retrying", 2)
    [(attempt, status, error)] = await env.executions(job_id)
    assert (attempt, status) == (1, "lost") and "stranded claim" in (error or "")


async def test_another_workers_claim_is_left_alone(env: Env):
    async def handler(payload: dict[str, Any], context: TaskContext) -> Any:
        return {}

    executor = env.executor(handler)
    job_id = await env.queued_job()
    await env.claim_as(job_id, uuid4(), "msg-2")  # running on some other worker
    disposition = await executor.handle(JobMessage(job_id=job_id, attempt=1, message_id="msg-2"))
    assert disposition is Disposition.ACK
    assert await env.job(job_id) == ("running", 1)
    assert [(a, s) for a, s, _ in await env.executions(job_id)] == [(1, "running")]


async def test_authority_lost_while_claiming_never_starts_the_handler(env: Env):
    authority = _authorized()

    class FenceDuringClaim:
        """Fences the worker as the claim's session opens."""

        def __init__(self) -> None:
            self.first = True

        def __call__(self) -> Any:
            if self.first:
                self.first = False
                authority.fence("test: lost during claim")
            return env.sf()

    async def handler(payload: dict[str, Any], context: TaskContext) -> Any:
        env.calls.append(context.attempt)
        return {}

    executor = env.executor(handler, session_factory=FenceDuringClaim())
    executor.authority = authority
    job_id = await env.queued_job()
    disposition = await executor.handle(JobMessage(job_id=job_id, attempt=1, message_id="m-3"))
    assert disposition is Disposition.ACK
    assert env.calls == []
    assert await env.job(job_id) == ("retrying", 2)
    assert [(a, s) for a, s, _ in await env.executions(job_id)] == [(1, "lost")]


async def test_handler_that_checks_authority_after_a_fence_is_handed_back(env: Env):
    authority = _authorized()
    side_effects: list[str] = []

    async def handler(payload: dict[str, Any], context: TaskContext) -> Any:
        authority.fence("test: partition while the handler runs")
        context.check_authority()  # raises: the side effect below never happens
        side_effects.append("sent")
        return {}

    executor = env.executor(handler)
    executor.authority = authority
    job_id = await env.queued_job()
    disposition = await executor.handle(JobMessage(job_id=job_id, attempt=1, message_id="m-4"))
    assert disposition is Disposition.ACK
    assert side_effects == []
    assert await env.job(job_id) == ("retrying", 2)
    [(attempt, status, error)] = await env.executions(job_id)
    assert (attempt, status) == (1, "lost") and "self-fenced" in (error or "")


async def test_without_authority_tracking_nothing_changes(env: Env):
    """Executors not owned by a self-fencing worker (Vercel path, older
    callers) behave as before."""

    async def handler(payload: dict[str, Any], context: TaskContext) -> Any:
        context.check_authority()  # no-op
        return {"ok": True}

    executor = env.executor(handler)
    job_id = await env.queued_job()
    disposition = await executor.handle(JobMessage(job_id=job_id, attempt=1, message_id="m-5"))
    assert disposition is Disposition.ACK
    assert await env.job(job_id) == ("completed", 1)
