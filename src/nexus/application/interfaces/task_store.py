import dataclasses
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from nexus.domain.scheduled_task import ScheduledTask
from nexus.domain.task_state import TaskInstance, TaskState, TaskTransitionRecord
from nexus.domain.task_submission import TaskResourceRequirements


@dataclass(frozen=True, slots=True)
class TaskRecord:
    """A persisted task: the state machine (TaskInstance), the scheduling
    metadata ScheduledTask needs, and the execution bookkeeping the runtime
    needs (attempt, lease, result). Immutable; changes are made with
    dataclasses.replace and written back through TaskStore.save()."""

    task_id: UUID
    tenant_id: str
    job_id: UUID
    task_type: str
    payload: dict[str, Any]
    state: TaskState
    attempt: int
    max_attempts: int
    initial_backoff_seconds: float
    max_backoff_seconds: float
    timeout_seconds: int
    fair_share_weight: float
    depends_on: frozenset[UUID] = frozenset()
    deadline: datetime | None = None
    not_before: datetime | None = None
    estimated_duration_seconds: float | None = None
    resource_requirements: dict[str, Any] | None = None
    idempotency_key: str | None = None
    result: Any = None
    last_error: str | None = None
    admission_reason: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    ready_since: datetime | None = None
    dispatch_requested_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    lease_expires_at: datetime | None = None
    version: int = 1

    @property
    def instance(self) -> TaskInstance:
        return TaskInstance(task_id=self.task_id, state=self.state)

    def with_instance(self, instance: TaskInstance, **changes: Any) -> "TaskRecord":
        return dataclasses.replace(self, state=instance.state, **changes)

    def to_scheduled_task(self) -> ScheduledTask:
        requirements = None
        if self.resource_requirements is not None:
            requirements = TaskResourceRequirements(**self.resource_requirements)
        return ScheduledTask(
            instance=self.instance,
            tenant_id=self.tenant_id,
            deadline=self.deadline,
            not_before=self.not_before,
            fair_share_weight=self.fair_share_weight,
            depends_on=self.depends_on,
            resource_requirements=requirements,
            ready_since=self.ready_since,
        )


@dataclass(frozen=True, slots=True)
class StoredTransition:
    attempt: int
    from_state: TaskState
    to_state: TaskState
    occurred_at: datetime
    reason: str | None


class TaskStore(Protocol):
    """Persistence port for tasks. Every method participates in the caller's
    transaction; the caller owns commit/rollback (same convention as the
    outbox and inbox ports)."""

    async def now(self) -> datetime:
        """The store's clock. All lease/expiry decisions use this single
        time source so replicas with skewed local clocks still agree."""
        ...

    async def insert_or_get_existing(self, record: TaskRecord) -> tuple[TaskRecord, bool]:
        """Insert `record`; if (tenant_id, idempotency_key) already exists,
        return the existing task and False instead."""
        ...

    async def get(self, task_id: UUID, *, tenant_id: str | None = None) -> TaskRecord | None: ...

    async def get_for_update(self, task_id: UUID) -> TaskRecord | None:
        """Row-locks the task until the transaction ends."""
        ...

    async def get_many(self, task_ids: Sequence[UUID]) -> list[TaskRecord]: ...

    async def list_for_tenant(
        self,
        tenant_id: str,
        *,
        state: TaskState | None,
        limit: int,
        created_before: datetime | None,
    ) -> list[TaskRecord]: ...

    async def save(self, record: TaskRecord) -> TaskRecord:
        """Persist mutable fields. Optimistic check on `version`; returns
        the record with the bumped version."""
        ...

    async def append_transitions(
        self, task_id: UUID, attempt: int, records: Sequence[TaskTransitionRecord]
    ) -> None: ...

    async def list_transitions(self, task_id: UUID) -> list[StoredTransition]: ...

    async def pending_dependents_for_update(self, task_id: UUID) -> list[TaskRecord]:
        """PENDING tasks that list `task_id` in depends_on, locked in a
        stable order (task_id) to avoid deadlocks between completions."""
        ...

    async def expired_leases_for_update(self, now: datetime, limit: int) -> list[TaskRecord]:
        """RUNNING tasks whose lease has lapsed (SKIP LOCKED)."""
        ...

    async def stale_ready_for_update(self, older_than: datetime, limit: int) -> list[TaskRecord]:
        """READY tasks whose last dispatch request is older than `older_than`
        (SKIP LOCKED)."""
        ...

    async def due_pending_for_update(self, now: datetime, limit: int) -> list[TaskRecord]:
        """PENDING tasks whose not_before has passed and whose dependencies
        all SUCCEEDED (SKIP LOCKED)."""
        ...

    async def blocked_pending_for_update(self, limit: int) -> list[TaskRecord]:
        """PENDING tasks with a dependency that can never succeed (SKIP LOCKED)."""
        ...
