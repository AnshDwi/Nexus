from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import Select, String, cast, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.application.interfaces.job_store import (
    ActivityEntry,
    JobExecution,
    JobQuery,
    Page,
    TrendBucket,
)
from nexus.domain.job import Job, JobPriority, JobState, JobTransition
from nexus.infrastructure.db.models.job import JobExecutionModel, JobModel, JobTransitionModel


class StaleJobVersionError(Exception):
    pass


_MUTABLE = (
    "state",
    "attempt",
    "max_attempts",
    "result",
    "last_error",
    "worker_id",
    "lease_expires_at",
    "queued_at",
    "started_at",
    "finished_at",
    "next_attempt_at",
    "dead_lettered_at",
)


def _to_job(m: JobModel) -> Job:
    return Job(
        job_id=m.job_id,
        tenant_id=m.tenant_id,
        name=m.name,
        job_type=m.job_type,
        payload=m.payload,
        priority=JobPriority(m.priority),
        state=JobState(m.state),
        attempt=m.attempt,
        max_attempts=m.max_attempts,
        retry_limit=m.retry_limit,
        initial_backoff_seconds=m.initial_backoff_seconds,
        max_backoff_seconds=m.max_backoff_seconds,
        timeout_seconds=m.timeout_seconds,
        created_at=m.created_at,
        updated_at=m.updated_at,
        idempotency_key=m.idempotency_key,
        correlation_id=m.correlation_id,
        result=m.result,
        last_error=m.last_error,
        worker_id=m.worker_id,
        lease_expires_at=m.lease_expires_at,
        queued_at=m.queued_at,
        started_at=m.started_at,
        finished_at=m.finished_at,
        next_attempt_at=m.next_attempt_at,
        dead_lettered_at=m.dead_lettered_at,
        version=m.version,
    )


def _value(v: Any) -> Any:
    return v.value if isinstance(v, (JobState, JobPriority)) else v


class PostgresJobStore:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def now(self) -> datetime:
        value: datetime = (await self._session.execute(select(func.clock_timestamp()))).scalar_one()
        return value

    async def insert_or_get_existing(self, job: Job) -> tuple[Job, bool]:
        values = {
            "job_id": job.job_id,
            "tenant_id": job.tenant_id,
            "name": job.name,
            "job_type": job.job_type,
            "payload": job.payload,
            "priority": job.priority.value,
            "retry_limit": job.retry_limit,
            "initial_backoff_seconds": job.initial_backoff_seconds,
            "max_backoff_seconds": job.max_backoff_seconds,
            "timeout_seconds": job.timeout_seconds,
            "idempotency_key": job.idempotency_key,
            "correlation_id": job.correlation_id,
            "created_at": job.created_at,
            "updated_at": job.updated_at,
            "version": job.version,
            **{name: _value(getattr(job, name)) for name in _MUTABLE},
        }
        stmt = (
            pg_insert(JobModel)
            .values(**values)
            .on_conflict_do_nothing(
                index_elements=[JobModel.tenant_id, JobModel.idempotency_key],
                index_where=JobModel.idempotency_key.is_not(None),
            )
            .returning(JobModel)
        )
        inserted = (await self._session.execute(stmt)).scalar_one_or_none()
        if inserted is not None:
            return _to_job(inserted), True
        existing = (
            await self._session.execute(
                select(JobModel).where(
                    JobModel.tenant_id == job.tenant_id,
                    JobModel.idempotency_key == job.idempotency_key,
                )
            )
        ).scalar_one()
        return _to_job(existing), False

    async def get(self, job_id: UUID, *, tenant_id: str | None = None) -> Job | None:
        stmt = select(JobModel).where(JobModel.job_id == job_id)
        if tenant_id is not None:
            stmt = stmt.where(JobModel.tenant_id == tenant_id)
        m = (await self._session.execute(stmt)).scalar_one_or_none()
        return _to_job(m) if m else None

    async def get_for_update(self, job_id: UUID) -> Job | None:
        stmt = select(JobModel).where(JobModel.job_id == job_id).with_for_update()
        m = (await self._session.execute(stmt)).scalar_one_or_none()
        return _to_job(m) if m else None

    async def save(self, job: Job, *, dispatch_requested_at: datetime | None = None) -> Job:
        values: dict[str, Any] = {name: _value(getattr(job, name)) for name in _MUTABLE}
        values["version"] = job.version + 1
        values["updated_at"] = job.updated_at
        if dispatch_requested_at is not None:
            values["dispatch_requested_at"] = dispatch_requested_at
        stmt = (
            update(JobModel)
            .where(JobModel.job_id == job.job_id, JobModel.version == job.version)
            .values(**values)
            .returning(JobModel)
        )
        m = (await self._session.execute(stmt)).scalar_one_or_none()
        if m is None:
            raise StaleJobVersionError(f"job {job.job_id} modified concurrently")
        return _to_job(m)

    async def append_transitions(self, job_id: UUID, transitions: Sequence[JobTransition]) -> None:
        for t in transitions:
            self._session.add(
                JobTransitionModel(
                    job_id=job_id,
                    attempt=t.attempt,
                    from_state=t.from_state.value,
                    to_state=t.to_state.value,
                    occurred_at=t.occurred_at,
                    reason=t.reason,
                )
            )
        await self._session.flush()

    async def list_transitions(self, job_id: UUID) -> list[JobTransition]:
        stmt = (
            select(JobTransitionModel)
            .where(JobTransitionModel.job_id == job_id)
            .order_by(JobTransitionModel.id)
        )
        return [
            JobTransition(
                from_state=JobState(m.from_state),
                to_state=JobState(m.to_state),
                occurred_at=m.occurred_at,
                attempt=m.attempt,
                reason=m.reason,
            )
            for m in (await self._session.execute(stmt)).scalars()
        ]

    async def search(self, query: JobQuery) -> Page[Job]:
        stmt = select(JobModel).where(JobModel.tenant_id == query.tenant_id)
        if query.states:
            stmt = stmt.where(JobModel.state.in_([s.value for s in query.states]))
        if query.priorities:
            stmt = stmt.where(JobModel.priority.in_([p.value for p in query.priorities]))
        if query.search:
            pattern = f"%{query.search.strip()}%"
            stmt = stmt.where(
                or_(
                    JobModel.name.ilike(pattern),
                    JobModel.job_type.ilike(pattern),
                    cast(JobModel.job_id, String).ilike(pattern),
                )
            )
        total = (
            await self._session.execute(select(func.count()).select_from(stmt.subquery()))
        ).scalar_one()
        stmt = (
            stmt.order_by(JobModel.created_at.desc(), JobModel.job_id)
            .offset((query.page - 1) * query.page_size)
            .limit(query.page_size)
        )
        items = [_to_job(m) for m in (await self._session.execute(stmt)).scalars()]
        return Page(items=items, total=total, page=query.page, page_size=query.page_size)

    async def start_execution(self, job: Job) -> None:
        stmt = (
            pg_insert(JobExecutionModel)
            .values(
                job_id=job.job_id,
                attempt=job.attempt,
                worker_id=job.worker_id,
                status="running",
                started_at=job.started_at,
            )
            .on_conflict_do_nothing(index_elements=["job_id", "attempt"])
        )
        await self._session.execute(stmt)

    async def finish_execution(
        self, job: Job, *, attempt: int, status: str, error: str | None, finished_at: datetime
    ) -> None:
        await self._session.execute(
            update(JobExecutionModel)
            .where(
                JobExecutionModel.job_id == job.job_id,
                JobExecutionModel.attempt == attempt,
                JobExecutionModel.status == "running",
            )
            .values(
                status=status,
                error=error,
                finished_at=finished_at,
                duration_ms=func.greatest(
                    0,
                    func.floor(
                        func.extract("epoch", finished_at - JobExecutionModel.started_at) * 1000
                    ),
                ),
            )
        )

    async def list_executions(
        self,
        tenant_id: str,
        *,
        job_id: UUID | None,
        status: str | None,
        page: int,
        page_size: int,
    ) -> Page[JobExecution]:
        stmt = (
            select(JobExecutionModel, JobModel.name)
            .join(JobModel, JobModel.job_id == JobExecutionModel.job_id)
            .where(JobModel.tenant_id == tenant_id)
        )
        if job_id is not None:
            stmt = stmt.where(JobExecutionModel.job_id == job_id)
        if status:
            stmt = stmt.where(JobExecutionModel.status == status)
        total = (
            await self._session.execute(select(func.count()).select_from(stmt.subquery()))
        ).scalar_one()
        stmt = (
            stmt.order_by(JobExecutionModel.started_at.desc(), JobExecutionModel.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        items = [
            JobExecution(
                execution_id=e.id,
                job_id=e.job_id,
                job_name=name,
                attempt=e.attempt,
                worker_id=e.worker_id,
                status=e.status,
                started_at=e.started_at,
                finished_at=e.finished_at,
                duration_ms=e.duration_ms,
                error=e.error,
            )
            for e, name in (await self._session.execute(stmt)).all()
        ]
        return Page(items=items, total=total, page=page, page_size=page_size)

    async def _locked(self, stmt: Select[tuple[JobModel]], limit: int) -> list[Job]:
        stmt = stmt.order_by(JobModel.updated_at).limit(limit).with_for_update(skip_locked=True)
        return [_to_job(m) for m in (await self._session.execute(stmt)).scalars()]

    async def running_on_workers_for_update(
        self, worker_ids: Sequence[UUID], limit: int
    ) -> list[Job]:
        return await self._locked(
            select(JobModel).where(
                JobModel.state == JobState.RUNNING.value,
                JobModel.worker_id.in_(list(worker_ids)),
            ),
            limit,
        )

    async def expired_leases_for_update(self, now: datetime, limit: int) -> list[Job]:
        return await self._locked(
            select(JobModel).where(
                JobModel.state == JobState.RUNNING.value, JobModel.lease_expires_at < now
            ),
            limit,
        )

    async def stale_dispatch_for_update(self, older_than: datetime, limit: int) -> list[Job]:
        """Dispatchable jobs whose last dispatch request is old and whose
        scheduled retry time (if any) has also passed long ago -- the
        dispatch message was lost or dead-lettered by the relay."""
        return await self._locked(
            select(JobModel).where(
                JobModel.state.in_(
                    [JobState.PENDING.value, JobState.RETRYING.value, JobState.QUEUED.value]
                ),
                or_(
                    JobModel.dispatch_requested_at.is_(None),
                    JobModel.dispatch_requested_at < older_than,
                ),
                or_(JobModel.next_attempt_at.is_(None), JobModel.next_attempt_at < older_than),
            ),
            limit,
        )

    async def counts_by_state(self, tenant_id: str) -> dict[JobState, int]:
        rows = await self._session.execute(
            select(JobModel.state, func.count())
            .where(JobModel.tenant_id == tenant_id)
            .group_by(JobModel.state)
        )
        counts = {state: 0 for state in JobState}
        for state, count in rows.all():
            counts[JobState(state)] = count
        return counts

    async def running_by_worker(self) -> dict[UUID, int]:
        rows = await self._session.execute(
            select(JobModel.worker_id, func.count())
            .where(JobModel.state == JobState.RUNNING.value, JobModel.worker_id.is_not(None))
            .group_by(JobModel.worker_id)
        )
        return {worker_id: count for worker_id, count in rows.all()}

    async def recent_activity(self, tenant_id: str, limit: int) -> list[ActivityEntry]:
        rows = await self._session.execute(
            select(JobTransitionModel, JobModel.name)
            .join(JobModel, JobModel.job_id == JobTransitionModel.job_id)
            .where(JobModel.tenant_id == tenant_id)
            .order_by(JobTransitionModel.id.desc())
            .limit(limit)
        )
        return [
            ActivityEntry(
                job_id=t.job_id,
                job_name=name,
                from_state=JobState(t.from_state),
                to_state=JobState(t.to_state),
                occurred_at=t.occurred_at,
                attempt=t.attempt,
                reason=t.reason,
            )
            for t, name in rows.all()
        ]

    async def execution_trend(
        self, tenant_id: str, *, since: datetime, bucket_minutes: int
    ) -> list[TrendBucket]:
        seconds = bucket_minutes * 60
        bucket = func.to_timestamp(
            func.floor(func.extract("epoch", JobExecutionModel.finished_at) / seconds) * seconds
        )
        rows = await self._session.execute(
            select(
                bucket.label("bucket"),
                func.count().filter(JobExecutionModel.status == "completed"),
                func.count().filter(JobExecutionModel.status != "completed"),
            )
            .join(JobModel, JobModel.job_id == JobExecutionModel.job_id)
            .where(
                JobModel.tenant_id == tenant_id,
                JobExecutionModel.finished_at.is_not(None),
                JobExecutionModel.finished_at >= since,
                JobExecutionModel.status != "cancelled",
            )
            .group_by("bucket")
            .order_by("bucket")
        )
        return [TrendBucket(bucket_start=b, completed=c, failed=f) for b, c, f in rows.all()]

    async def queue_snapshot(self, tenant_id: str, limit: int) -> list[dict[str, Any]]:
        # Position in this string orders critical < high < normal < low.
        rank = func.coalesce(
            func.nullif(
                func.strpos("critical,high,normal,low", JobModel.priority),
                0,
            ),
            99,
        )
        rows = await self._session.execute(
            select(JobModel)
            .where(
                JobModel.tenant_id == tenant_id,
                JobModel.state.in_(
                    [JobState.PENDING.value, JobState.QUEUED.value, JobState.RETRYING.value]
                ),
            )
            .order_by(rank, JobModel.created_at)
            .limit(limit)
        )
        return [
            {
                "job": _to_job(m),
                "dispatch_requested_at": m.dispatch_requested_at,
            }
            for m in rows.scalars()
        ]
