"""C2: the worker keeps trying to record a job's outcome until it commits,
the database rejects it (fenced), or the attempt's lease deadline passes.

Real PostgreSQL; the database "outage" is a session factory that fails on
demand, switched on by the job's handler itself, so it starts exactly
between execution and reporting.
"""

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from nexus.application.interfaces.task_handler import PermanentTaskError, TaskContext
from nexus.domain.job import Job, JobPriority, JobState
from nexus.infrastructure.config import get_settings
from nexus.infrastructure.db.repositories.job_repository import PostgresJobStore
from nexus.infrastructure.db.session import create_engine, create_session_factory
from nexus.infrastructure.execution import job_executor as job_executor_module
from nexus.infrastructure.execution.handlers import InMemoryTaskHandlerRegistry
from nexus.infrastructure.execution.job_executor import Disposition, JobExecutor, JobMessage

pytestmark = pytest.mark.integration


class _Unreachable:
    async def __aenter__(self) -> Any:
        raise ConnectionRefusedError("simulated: database unreachable")

    async def __aexit__(self, *exc: object) -> None:
        return None


class _CommitThenDrop:
    """The COMMIT reaches the database, but the reply is lost."""

    def __init__(self, cm: Any) -> None:
        self._cm = cm

    async def __aenter__(self) -> Any:
        session = await self._cm.__aenter__()
        real_commit = session.commit

        async def commit() -> None:
            await real_commit()
            raise ConnectionResetError("simulated: connection lost after COMMIT")

        session.commit = commit
        return session

    async def __aexit__(self, *exc: object) -> Any:
        return await self._cm.__aexit__(*exc)


class FlakySessions:
    def __init__(self, real: Any) -> None:
        self._real = real
        self.fail_next = 0
        self.drop_after_next_commit = False

    def __call__(self) -> Any:
        if self.fail_next > 0:
            self.fail_next -= 1
            return _Unreachable()
        if self.drop_after_next_commit:
            self.drop_after_next_commit = False
            return _CommitThenDrop(self._real())
        return self._real()


@dataclass
class Env:
    sf: Any
    flaky: FlakySessions
    calls: list[int]

    def executor(self, handler: Any, *, lease_grace: float = 30) -> JobExecutor:
        return JobExecutor(
            self.flaky,  # type: ignore[arg-type]
            InMemoryTaskHandlerRegistry({"probe": handler}),
            worker_id=uuid4(),
            lease_grace=timedelta(seconds=lease_grace),
        )

    async def queued_job(self, *, timeout: int = 60, max_attempts: int = 3) -> UUID:
        now = datetime.now(UTC)
        job = Job(
            job_id=uuid4(),
            tenant_id="acme",
            name="report",
            job_type="probe",
            payload={},
            priority=JobPriority.NORMAL,
            state=JobState.QUEUED,
            attempt=1,
            max_attempts=max_attempts,
            retry_limit=max_attempts,
            initial_backoff_seconds=0,
            max_backoff_seconds=0,
            timeout_seconds=timeout,
            created_at=now,
            updated_at=now,
        )
        async with self.sf() as session:
            await PostgresJobStore(session).insert_or_get_existing(job)
            await session.commit()
        return job.job_id

    async def job(self, job_id: UUID) -> tuple[str, int]:
        async with self.sf() as session:
            row = (
                await session.execute(
                    text("SELECT state, attempt FROM jobs WHERE job_id = :j"), {"j": job_id}
                )
            ).one()
        return row[0], row[1]

    async def executions(self, job_id: UUID) -> list[tuple[int, str]]:
        async with self.sf() as session:
            rows = await session.execute(
                text("SELECT attempt, status FROM job_executions WHERE job_id = :j ORDER BY id"),
                {"j": job_id},
            )
            return [(r[0], r[1]) for r in rows]


@pytest.fixture
async def env(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Env]:
    # Same algorithm, shorter waits, so the tests stay fast.
    monkeypatch.setattr(job_executor_module, "_REPORT_INITIAL_DELAY", 0.05)
    monkeypatch.setattr(job_executor_module, "_REPORT_MAX_DELAY", 0.2)
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
    yield Env(sf, FlakySessions(sf), [])
    await engine.dispose()
    get_settings.cache_clear()


def _message(job_id: UUID) -> JobMessage:
    return JobMessage(job_id=job_id, attempt=1, message_id=f"msg-{uuid4()}")


async def test_result_is_recorded_after_a_blip_longer_than_the_old_retry_budget(env: Env):
    """The old code gave up after 3 tries; 6 consecutive failures must not
    lose the result."""

    async def handler(payload: dict[str, Any], context: TaskContext) -> Any:
        env.calls.append(context.attempt)
        env.flaky.fail_next = 6  # PostgreSQL disappears as the handler returns
        return {"ok": True}

    job_id = await env.queued_job()
    disposition = await env.executor(handler).handle(_message(job_id))
    assert disposition is Disposition.ACK
    assert env.calls == [1]
    assert await env.job(job_id) == ("completed", 1)
    assert await env.executions(job_id) == [(1, "completed")]


async def test_failure_report_is_also_retried(env: Env):
    async def handler(payload: dict[str, Any], context: TaskContext) -> Any:
        env.flaky.fail_next = 6
        raise RuntimeError("boom")

    job_id = await env.queued_job()
    disposition = await env.executor(handler).handle(_message(job_id))
    assert disposition is Disposition.ACK
    assert await env.job(job_id) == ("retrying", 2)
    assert await env.executions(job_id) == [(1, "failed")]


async def test_reporting_stops_at_the_attempts_lease_deadline(env: Env):
    """If PostgreSQL never comes back, the worker stops at the lease deadline
    (timeout 1 s + grace 0 s) and leaves the attempt to the scheduler."""

    async def handler(payload: dict[str, Any], context: TaskContext) -> Any:
        env.flaky.fail_next = 10**6
        return {"ok": True}

    job_id = await env.queued_job(timeout=1)
    started = datetime.now(UTC)
    disposition = await env.executor(handler, lease_grace=0).handle(_message(job_id))
    elapsed = (datetime.now(UTC) - started).total_seconds()
    assert disposition is Disposition.ACK
    assert elapsed < 5, f"reporting did not stop at the deadline ({elapsed:.1f}s)"
    env.flaky.fail_next = 0
    assert await env.job(job_id) == ("running", 1)  # untouched: the scheduler's to recover


async def test_ambiguous_success_commit_is_read_back_not_called_fenced(
    env: Env, caplog: pytest.LogCaptureFixture
):
    async def handler(payload: dict[str, Any], context: TaskContext) -> Any:
        env.flaky.drop_after_next_commit = True
        return {"ok": True}

    job_id = await env.queued_job()
    with caplog.at_level(logging.INFO, logger="nexus.jobs.executor"):
        assert await env.executor(handler).handle(_message(job_id)) is Disposition.ACK
    finished = [r for r in caplog.records if r.getMessage() == "job finished"]
    assert [getattr(r, "state", None) for r in finished] == ["completed"]
    assert await env.job(job_id) == ("completed", 1)
    assert await env.executions(job_id) == [(1, "completed")]


async def test_ambiguous_final_failure_commit_still_dead_letters_the_message(env: Env):
    async def handler(payload: dict[str, Any], context: TaskContext) -> Any:
        env.flaky.drop_after_next_commit = True
        raise PermanentTaskError("bad input")

    job_id = await env.queued_job(max_attempts=1)
    assert await env.executor(handler).handle(_message(job_id)) is Disposition.DEAD_LETTER
    assert await env.job(job_id) == ("failed", 1)
