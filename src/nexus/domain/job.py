"""The Job aggregate: the user-facing unit of work in NEXUS.

Lifecycle (single source of truth: JOB_TRANSITIONS):

    PENDING ──► QUEUED ──► RUNNING ──► COMPLETED
                  ▲           │
                  │           ▼
               RETRYING ◄── FAILED   (automatic while attempts remain,
                                      or a manual retry of a final failure)

    PENDING / QUEUED / RETRYING / RUNNING ──► CANCELLED

* PENDING  -> QUEUED only once the messaging system has *confirmed* the
  dispatch message (publisher confirm), never optimistically.
* FAILED is final when attempts are exhausted (the job is dead-lettered);
  a manual retry moves it to RETRYING with a fresh attempt budget.
* COMPLETED and CANCELLED are terminal.

Everything here is pure: callers pass `now`, and every change yields a
JobTransition for the audit log.
"""

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Self
from uuid import UUID

from nexus.domain.task_lifecycle import retry_backoff


class JobState(StrEnum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


class JobPriority(StrEnum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"

    @property
    def broker_priority(self) -> int:
        """AMQP message priority (queue declared with x-max-priority=10)."""
        return {"low": 1, "normal": 4, "high": 7, "critical": 10}[self.value]


JOB_TRANSITIONS: dict[JobState, frozenset[JobState]] = {
    JobState.PENDING: frozenset({JobState.QUEUED, JobState.CANCELLED}),
    JobState.QUEUED: frozenset({JobState.RUNNING, JobState.CANCELLED}),
    JobState.RUNNING: frozenset({JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED}),
    JobState.FAILED: frozenset({JobState.RETRYING}),
    JobState.RETRYING: frozenset({JobState.QUEUED, JobState.CANCELLED}),
    JobState.COMPLETED: frozenset(),
    JobState.CANCELLED: frozenset(),
}
# If the broker accepted a message but the dispatcher crashed before
# recording QUEUED, the worker that receives it records the missing
# PENDING/RETRYING -> QUEUED step itself before RUNNING (see Job.start).

TERMINAL_JOB_STATES = frozenset({JobState.COMPLETED, JobState.CANCELLED})
DISPATCHABLE_JOB_STATES = frozenset({JobState.PENDING, JobState.RETRYING, JobState.QUEUED})
CANCELLABLE_JOB_STATES = frozenset(
    {JobState.PENDING, JobState.QUEUED, JobState.RETRYING, JobState.RUNNING}
)


class InvalidJobTransitionError(Exception):
    def __init__(self, from_state: JobState, to_state: JobState) -> None:
        super().__init__(f"Cannot transition job from '{from_state.value}' to '{to_state.value}'")
        self.from_state = from_state
        self.to_state = to_state


class JobNotRetryableError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class JobTransition:
    from_state: JobState
    to_state: JobState
    occurred_at: datetime
    attempt: int
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class Job:
    job_id: UUID
    tenant_id: str
    name: str
    job_type: str
    payload: dict[str, Any]
    priority: JobPriority
    state: JobState
    attempt: int
    max_attempts: int
    retry_limit: int
    initial_backoff_seconds: float
    max_backoff_seconds: float
    timeout_seconds: int
    created_at: datetime
    updated_at: datetime
    idempotency_key: str | None = None
    correlation_id: str | None = None
    result: Any = None
    last_error: str | None = None
    worker_id: UUID | None = None
    lease_expires_at: datetime | None = None
    queued_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    next_attempt_at: datetime | None = None
    dead_lettered_at: datetime | None = None
    version: int = 1

    @property
    def attempts_remaining(self) -> int:
        return max(self.max_attempts - self.attempt, 0)

    def _to(
        self, state: JobState, *, now: datetime, reason: str | None, **changes: Any
    ) -> tuple[Self, JobTransition]:
        if state not in JOB_TRANSITIONS[self.state]:
            raise InvalidJobTransitionError(self.state, state)
        transition = JobTransition(
            from_state=self.state,
            to_state=state,
            occurred_at=now,
            attempt=changes.get("attempt", self.attempt),
            reason=reason,
        )
        return replace(self, state=state, updated_at=now, **changes), transition

    # --- dispatch ---------------------------------------------------------
    def mark_queued(self, *, now: datetime) -> tuple[Self, JobTransition]:
        """The broker confirmed the dispatch message for the current attempt."""
        return self._to(
            JobState.QUEUED, now=now, reason="dispatch confirmed by broker", queued_at=now
        )

    # --- execution --------------------------------------------------------
    def start(
        self, *, worker_id: UUID, now: datetime, lease_grace: timedelta
    ) -> tuple[Self, list[JobTransition]]:
        transitions: list[JobTransition] = []
        job: Self = self
        if job.state in (JobState.PENDING, JobState.RETRYING):
            job, t = job._to(
                JobState.QUEUED, now=now, reason="dispatch observed by worker", queued_at=now
            )
            transitions.append(t)
        job, t = job._to(
            JobState.RUNNING,
            now=now,
            reason=f"started by worker {worker_id}",
            worker_id=worker_id,
            started_at=now,
            finished_at=None,
            lease_expires_at=now + timedelta(seconds=self.timeout_seconds) + lease_grace,
        )
        transitions.append(t)
        return job, transitions

    def complete(self, *, result: Any, now: datetime) -> tuple[Self, JobTransition]:
        return self._to(
            JobState.COMPLETED,
            now=now,
            reason="completed",
            result=result,
            finished_at=now,
            lease_expires_at=None,
            last_error=None,
        )

    def fail(
        self, *, error: str, now: datetime, permanent: bool = False
    ) -> tuple[Self, list[JobTransition]]:
        """RUNNING -> FAILED, then -> RETRYING with exponential backoff while
        attempts remain (and the error is not permanent). When exhausted the
        job stays FAILED and is marked dead-lettered."""
        failed, t1 = self._to(
            JobState.FAILED,
            now=now,
            reason=error,
            last_error=error,
            finished_at=now,
            lease_expires_at=None,
        )
        if permanent or self.attempt >= self.max_attempts:
            reason = "permanent error" if permanent else f"attempts exhausted ({self.attempt})"
            return replace(failed, dead_lettered_at=now, last_error=f"{error} [{reason}]"), [t1]
        backoff = retry_backoff(
            self.attempt,
            initial_backoff_seconds=self.initial_backoff_seconds,
            max_backoff_seconds=self.max_backoff_seconds,
        )
        retrying, t2 = failed._to(
            JobState.RETRYING,
            now=now,
            reason=f"retry {self.attempt + 1}/{self.max_attempts} in {backoff.total_seconds():g}s",
            attempt=self.attempt + 1,
            next_attempt_at=now + backoff,
            worker_id=None,
        )
        return retrying, [t1, t2]

    def retry_manually(self, *, now: datetime) -> tuple[Self, JobTransition]:
        """Operator retry of a final failure: a fresh budget of retry_limit
        attempts, dispatched immediately."""
        if self.state is not JobState.FAILED:
            raise JobNotRetryableError(f"only failed jobs can be retried (job is {self.state})")
        return self._to(
            JobState.RETRYING,
            now=now,
            reason="manual retry",
            attempt=self.attempt + 1,
            max_attempts=self.attempt + self.retry_limit,
            next_attempt_at=now,
            dead_lettered_at=None,
            worker_id=None,
        )

    def cancel(self, *, now: datetime, reason: str) -> tuple[Self, JobTransition]:
        return self._to(
            JobState.CANCELLED,
            now=now,
            reason=reason,
            finished_at=now,
            lease_expires_at=None,
            last_error=reason if self.state is JobState.RUNNING else self.last_error,
        )
