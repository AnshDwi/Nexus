from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import Select, any_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from nexus.application.interfaces.task_store import StoredTransition, TaskRecord
from nexus.domain.task_state import TaskState, TaskTransitionRecord
from nexus.infrastructure.db.models.task import TaskModel, TaskTransitionModel

_BLOCKING_STATES = (TaskState.PERMANENTLY_FAILED, TaskState.CANCELLED, TaskState.REJECTED)


class StaleTaskVersionError(Exception):
    def __init__(self, task_id: UUID, version: int) -> None:
        super().__init__(f"Task {task_id} was modified concurrently (expected version {version})")
        self.task_id = task_id
        self.version = version


_MUTABLE_FIELDS = (
    "state",
    "attempt",
    "payload",
    "not_before",
    "result",
    "last_error",
    "admission_reason",
    "ready_since",
    "dispatch_requested_at",
    "started_at",
    "finished_at",
    "lease_expires_at",
)


def _to_record(model: TaskModel) -> TaskRecord:
    return TaskRecord(
        task_id=model.task_id,
        tenant_id=model.tenant_id,
        job_id=model.job_id,
        task_type=model.task_type,
        payload=model.payload,
        state=TaskState(model.state),
        attempt=model.attempt,
        max_attempts=model.max_attempts,
        initial_backoff_seconds=model.initial_backoff_seconds,
        max_backoff_seconds=model.max_backoff_seconds,
        timeout_seconds=model.timeout_seconds,
        fair_share_weight=model.fair_share_weight,
        depends_on=frozenset(model.depends_on or ()),
        deadline=model.deadline,
        not_before=model.not_before,
        estimated_duration_seconds=model.estimated_duration_seconds,
        resource_requirements=model.resource_requirements,
        idempotency_key=model.idempotency_key,
        result=model.result,
        last_error=model.last_error,
        admission_reason=model.admission_reason,
        created_at=model.created_at,
        updated_at=model.updated_at,
        ready_since=model.ready_since,
        dispatch_requested_at=model.dispatch_requested_at,
        started_at=model.started_at,
        finished_at=model.finished_at,
        lease_expires_at=model.lease_expires_at,
        version=model.version,
    )


def _insert_values(record: TaskRecord) -> dict[str, Any]:
    values: dict[str, Any] = {
        "task_id": record.task_id,
        "tenant_id": record.tenant_id,
        "job_id": record.job_id,
        "task_type": record.task_type,
        "max_attempts": record.max_attempts,
        "initial_backoff_seconds": record.initial_backoff_seconds,
        "max_backoff_seconds": record.max_backoff_seconds,
        "timeout_seconds": record.timeout_seconds,
        "fair_share_weight": record.fair_share_weight,
        "depends_on": sorted(record.depends_on, key=str),
        "deadline": record.deadline,
        "estimated_duration_seconds": record.estimated_duration_seconds,
        "resource_requirements": record.resource_requirements,
        "idempotency_key": record.idempotency_key,
        "version": record.version,
    }
    for name in _MUTABLE_FIELDS:
        value = getattr(record, name)
        values[name] = value.value if isinstance(value, TaskState) else value
    if record.created_at is not None:
        values["created_at"] = record.created_at
        values["updated_at"] = record.updated_at or record.created_at
    return values


class PostgresTaskStore:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def now(self) -> datetime:
        # clock_timestamp(), not now(): now() is frozen at transaction start,
        # and callers compare against leases set by other transactions.
        value: datetime = (await self._session.execute(select(func.clock_timestamp()))).scalar_one()
        return value

    async def insert_or_get_existing(self, record: TaskRecord) -> tuple[TaskRecord, bool]:
        stmt = (
            pg_insert(TaskModel)
            .values(**_insert_values(record))
            .on_conflict_do_nothing(
                index_elements=[TaskModel.tenant_id, TaskModel.idempotency_key],
                index_where=TaskModel.idempotency_key.is_not(None),
            )
            .returning(TaskModel)
        )
        inserted = (await self._session.execute(stmt)).scalar_one_or_none()
        if inserted is not None:
            return _to_record(inserted), True

        existing = (
            await self._session.execute(
                select(TaskModel).where(
                    TaskModel.tenant_id == record.tenant_id,
                    TaskModel.idempotency_key == record.idempotency_key,
                )
            )
        ).scalar_one()
        return _to_record(existing), False

    async def get(self, task_id: UUID, *, tenant_id: str | None = None) -> TaskRecord | None:
        stmt = select(TaskModel).where(TaskModel.task_id == task_id)
        if tenant_id is not None:
            stmt = stmt.where(TaskModel.tenant_id == tenant_id)
        model = (await self._session.execute(stmt)).scalar_one_or_none()
        return _to_record(model) if model is not None else None

    async def get_for_update(self, task_id: UUID) -> TaskRecord | None:
        stmt = select(TaskModel).where(TaskModel.task_id == task_id).with_for_update()
        model = (await self._session.execute(stmt)).scalar_one_or_none()
        return _to_record(model) if model is not None else None

    async def get_many(self, task_ids: Sequence[UUID]) -> list[TaskRecord]:
        if not task_ids:
            return []
        stmt = select(TaskModel).where(TaskModel.task_id.in_(list(task_ids)))
        return [_to_record(m) for m in (await self._session.execute(stmt)).scalars()]

    async def list_for_tenant(
        self,
        tenant_id: str,
        *,
        state: TaskState | None,
        limit: int,
        created_before: datetime | None,
    ) -> list[TaskRecord]:
        stmt = select(TaskModel).where(TaskModel.tenant_id == tenant_id)
        if state is not None:
            stmt = stmt.where(TaskModel.state == state.value)
        if created_before is not None:
            stmt = stmt.where(TaskModel.created_at < created_before)
        stmt = stmt.order_by(TaskModel.created_at.desc(), TaskModel.task_id).limit(limit)
        return [_to_record(m) for m in (await self._session.execute(stmt)).scalars()]

    async def save(self, record: TaskRecord) -> TaskRecord:
        values: dict[str, Any] = {}
        for name in _MUTABLE_FIELDS:
            value = getattr(record, name)
            values[name] = value.value if isinstance(value, TaskState) else value
        values["version"] = record.version + 1
        values["updated_at"] = func.clock_timestamp()
        stmt = (
            update(TaskModel)
            .where(TaskModel.task_id == record.task_id, TaskModel.version == record.version)
            .values(**values)
            .returning(TaskModel)
        )
        model = (await self._session.execute(stmt)).scalar_one_or_none()
        if model is None:
            raise StaleTaskVersionError(record.task_id, record.version)
        return _to_record(model)

    async def append_transitions(
        self, task_id: UUID, attempt: int, records: Sequence[TaskTransitionRecord]
    ) -> None:
        for record in records:
            self._session.add(
                TaskTransitionModel(
                    task_id=task_id,
                    attempt=attempt,
                    from_state=record.from_state.value,
                    to_state=record.to_state.value,
                    occurred_at=record.occurred_at,
                    reason=record.reason,
                )
            )
        await self._session.flush()

    async def list_transitions(self, task_id: UUID) -> list[StoredTransition]:
        stmt = (
            select(TaskTransitionModel)
            .where(TaskTransitionModel.task_id == task_id)
            .order_by(TaskTransitionModel.id)
        )
        return [
            StoredTransition(
                attempt=m.attempt,
                from_state=TaskState(m.from_state),
                to_state=TaskState(m.to_state),
                occurred_at=m.occurred_at,
                reason=m.reason,
            )
            for m in (await self._session.execute(stmt)).scalars()
        ]

    async def pending_dependents_for_update(self, task_id: UUID) -> list[TaskRecord]:
        stmt = (
            select(TaskModel)
            .where(
                TaskModel.depends_on.contains([task_id]),
                TaskModel.state == TaskState.PENDING.value,
            )
            .order_by(TaskModel.task_id)
            .with_for_update()
        )
        return [_to_record(m) for m in (await self._session.execute(stmt)).scalars()]

    async def _locked_batch(self, stmt: Select[tuple[TaskModel]], limit: int) -> list[TaskRecord]:
        stmt = stmt.order_by(TaskModel.updated_at).limit(limit).with_for_update(skip_locked=True)
        return [_to_record(m) for m in (await self._session.execute(stmt)).scalars()]

    async def expired_leases_for_update(self, now: datetime, limit: int) -> list[TaskRecord]:
        return await self._locked_batch(
            select(TaskModel).where(
                TaskModel.state == TaskState.RUNNING.value,
                TaskModel.lease_expires_at < now,
            ),
            limit,
        )

    async def stale_ready_for_update(self, older_than: datetime, limit: int) -> list[TaskRecord]:
        return await self._locked_batch(
            select(TaskModel).where(
                TaskModel.state == TaskState.READY.value,
                or_(
                    TaskModel.dispatch_requested_at.is_(None),
                    TaskModel.dispatch_requested_at < older_than,
                ),
                # A retry deferred by backoff is not stale until its
                # scheduled delivery time is itself older than the cutoff.
                or_(TaskModel.not_before.is_(None), TaskModel.not_before < older_than),
            ),
            limit,
        )

    async def due_pending_for_update(self, now: datetime, limit: int) -> list[TaskRecord]:
        """PENDING tasks that are promotable right now: not_before reached and
        every dependency SUCCEEDED."""
        dep = aliased(TaskModel)
        unmet_dependency = (
            select(dep.task_id)
            .where(
                dep.task_id == any_(TaskModel.depends_on),
                dep.state != TaskState.SUCCEEDED.value,
            )
            .exists()
        )
        return await self._locked_batch(
            select(TaskModel).where(
                TaskModel.state == TaskState.PENDING.value,
                or_(TaskModel.not_before.is_(None), TaskModel.not_before <= now),
                ~unmet_dependency,
            ),
            limit,
        )

    async def blocked_pending_for_update(self, limit: int) -> list[TaskRecord]:
        """PENDING tasks with a dependency that can never succeed."""
        dep = aliased(TaskModel)
        blocking_dependency = (
            select(dep.task_id)
            .where(
                dep.task_id == any_(TaskModel.depends_on),
                dep.state.in_([s.value for s in _BLOCKING_STATES]),
            )
            .exists()
        )
        return await self._locked_batch(
            select(TaskModel).where(
                TaskModel.state == TaskState.PENDING.value, blocking_dependency
            ),
            limit,
        )
