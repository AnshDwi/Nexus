"""Pure lifecycle decisions for executing a task: claiming, succeeding,
failing with retry/backoff, and cancelling.

Every state change goes through TaskInstance.transition_to, so the single
transition table in task_state.py stays the only authority on what is legal.
These functions only *plan* changes; persisting them is the caller's job.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from nexus.domain.task_state import TaskInstance, TaskState, TaskTransitionRecord


class FailureDecision(StrEnum):
    RETRY = "retry"
    GIVE_UP = "give_up"


def retry_backoff(
    attempt: int, *, initial_backoff_seconds: float, max_backoff_seconds: float
) -> timedelta:
    """Exponential backoff after failed attempt number `attempt` (1-based):
    initial * 2^(attempt-1), capped at max."""
    if attempt < 1:
        raise ValueError("attempt is 1-based")
    seconds = min(initial_backoff_seconds * (2 ** (attempt - 1)), max_backoff_seconds)
    return timedelta(seconds=max(seconds, 0.0))


def decide_after_failure(*, attempt: int, max_attempts: int) -> FailureDecision:
    return FailureDecision.RETRY if attempt < max_attempts else FailureDecision.GIVE_UP


@dataclass(frozen=True, slots=True)
class ClaimPlan:
    instance: TaskInstance
    records: tuple[TaskTransitionRecord, ...]
    lease_expires_at: datetime


def plan_claim(
    instance: TaskInstance, *, now: datetime, timeout: timedelta, lease_grace: timedelta
) -> ClaimPlan:
    """READY -> DISPATCHED -> RUNNING for a worker that has accepted the task.

    The lease covers the execution timeout plus grace; once it lapses, the
    task is treated as crashed and becomes eligible for retry."""
    dispatched, first = instance.transition_to(
        TaskState.DISPATCHED, occurred_at=now, reason="accepted by worker"
    )
    running, second = dispatched.transition_to(
        TaskState.RUNNING, occurred_at=now, reason="execution started"
    )
    return ClaimPlan(
        instance=running, records=(first, second), lease_expires_at=now + timeout + lease_grace
    )


@dataclass(frozen=True, slots=True)
class FailurePlan:
    instance: TaskInstance
    records: tuple[TaskTransitionRecord, ...]
    decision: FailureDecision
    next_attempt: int | None
    retry_not_before: datetime | None


def plan_failure(
    instance: TaskInstance,
    *,
    attempt: int,
    max_attempts: int,
    initial_backoff_seconds: float,
    max_backoff_seconds: float,
    now: datetime,
    reason: str,
) -> FailurePlan:
    """RUNNING -> FAILED, then FAILED -> READY (retry after backoff) or
    FAILED -> PERMANENTLY_FAILED once attempts are exhausted."""
    failed, failed_record = instance.transition_to(TaskState.FAILED, occurred_at=now, reason=reason)
    decision = decide_after_failure(attempt=attempt, max_attempts=max_attempts)
    if decision is FailureDecision.RETRY:
        backoff = retry_backoff(
            attempt,
            initial_backoff_seconds=initial_backoff_seconds,
            max_backoff_seconds=max_backoff_seconds,
        )
        retried, retry_record = failed.transition_to(
            TaskState.READY,
            occurred_at=now,
            reason=f"retry {attempt + 1}/{max_attempts} after {backoff.total_seconds():g}s",
        )
        return FailurePlan(
            instance=retried,
            records=(failed_record, retry_record),
            decision=decision,
            next_attempt=attempt + 1,
            retry_not_before=now + backoff,
        )

    exhausted, exhausted_record = failed.transition_to(
        TaskState.PERMANENTLY_FAILED,
        occurred_at=now,
        reason=f"attempts exhausted ({attempt}/{max_attempts})",
    )
    return FailurePlan(
        instance=exhausted,
        records=(failed_record, exhausted_record),
        decision=decision,
        next_attempt=None,
        retry_not_before=None,
    )


def plan_success(
    instance: TaskInstance, *, now: datetime
) -> tuple[TaskInstance, TaskTransitionRecord]:
    return instance.transition_to(TaskState.SUCCEEDED, occurred_at=now, reason="completed")


def plan_cancel(
    instance: TaskInstance, *, now: datetime, reason: str
) -> tuple[TaskInstance, TaskTransitionRecord]:
    """Raises InvalidTaskTransitionError for terminal tasks."""
    return instance.transition_to(TaskState.CANCELLED, occurred_at=now, reason=reason)


def blocks_dependents(state: TaskState) -> bool:
    """A dependency in one of these states can never succeed, so tasks
    waiting on it must be cancelled rather than left PENDING forever."""
    return state in {TaskState.PERMANENTLY_FAILED, TaskState.CANCELLED, TaskState.REJECTED}
