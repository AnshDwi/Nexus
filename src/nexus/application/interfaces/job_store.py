from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from nexus.domain.job import Job, JobPriority, JobState, JobTransition


@dataclass(frozen=True, slots=True)
class JobQuery:
    tenant_id: str
    states: frozenset[JobState] = frozenset()
    priorities: frozenset[JobPriority] = frozenset()
    search: str | None = None
    dead_lettered: bool | None = None
    error: str | None = None  # case-insensitive substring of last_error
    page: int = 1
    page_size: int = 25


@dataclass(frozen=True, slots=True)
class Page[T]:
    items: list[T]
    total: int
    page: int
    page_size: int


@dataclass(frozen=True, slots=True)
class JobExecution:
    """One attempt of one job on one worker (the Execution History)."""

    execution_id: int
    job_id: UUID
    job_name: str
    attempt: int
    worker_id: UUID | None
    status: str  # running | completed | failed | timed_out | lost | cancelled
    started_at: datetime
    finished_at: datetime | None
    duration_ms: int | None
    error: str | None


@dataclass(frozen=True, slots=True)
class ActivityEntry:
    job_id: UUID
    job_name: str
    from_state: JobState
    to_state: JobState
    occurred_at: datetime
    attempt: int
    reason: str | None


@dataclass(frozen=True, slots=True)
class TrendBucket:
    bucket_start: datetime
    completed: int
    failed: int


class JobStore(Protocol):
    """Persistence port for jobs. Participates in the caller's transaction."""

    async def now(self) -> datetime: ...

    async def insert_or_get_existing(self, job: Job) -> tuple[Job, bool]: ...

    async def get(self, job_id: UUID, *, tenant_id: str | None = None) -> Job | None: ...

    async def get_for_update(self, job_id: UUID) -> Job | None: ...

    async def save(self, job: Job, *, dispatch_requested_at: datetime | None = None) -> Job: ...

    async def append_transitions(
        self, job_id: UUID, transitions: Sequence[JobTransition]
    ) -> None: ...

    async def list_transitions(self, job_id: UUID) -> list[JobTransition]: ...

    async def search(self, query: JobQuery) -> Page[Job]: ...

    async def start_execution(self, job: Job) -> None: ...

    async def finish_execution(
        self, job: Job, *, attempt: int, status: str, error: str | None, finished_at: datetime
    ) -> None: ...

    async def list_executions(
        self,
        tenant_id: str,
        *,
        job_id: UUID | None,
        status: str | None,
        page: int,
        page_size: int,
    ) -> Page[JobExecution]: ...

    # --- scheduler (leader only; SKIP LOCKED) -------------------------------
    async def running_on_workers_for_update(
        self, worker_ids: Sequence[UUID], limit: int
    ) -> list[Job]: ...

    async def expired_leases_for_update(self, now: datetime, limit: int) -> list[Job]: ...

    async def stale_dispatch_for_update(self, older_than: datetime, limit: int) -> list[Job]: ...

    # --- read models (dashboard) --------------------------------------------
    async def counts_by_state(self, tenant_id: str) -> dict[JobState, int]: ...

    async def count_dead_lettered(self, tenant_id: str) -> int: ...

    async def running_by_worker(self) -> dict[UUID, int]: ...

    async def recent_activity(self, tenant_id: str, limit: int) -> list[ActivityEntry]: ...

    async def execution_trend(
        self, tenant_id: str, *, since: datetime, bucket_minutes: int
    ) -> list[TrendBucket]: ...

    async def queue_snapshot(self, tenant_id: str, limit: int) -> list[dict[str, Any]]: ...
