"""Task execution lifecycle use cases: claim, complete, fail, cancel, and the
dependency bookkeeping that follows each outcome.

Delivery guarantees these functions provide, given at-least-once delivery of
dispatch messages:

* A task attempt is executed by at most one worker at a time. The claim moves
  READY -> RUNNING under a row lock and only for the attempt number carried
  by the message; duplicate or stale deliveries find the task no longer READY
  (or on a different attempt) and are acknowledged without running anything.
* A result is only recorded for the attempt that is still RUNNING. A worker
  whose lease expired (and whose attempt was retried) cannot overwrite the
  newer attempt's outcome -- the attempt number acts as a per-task fencing
  token.
* Nothing is lost if a worker dies mid-execution: the dispatch message is
  redelivered while the lease is live (the claim defers it), and once the
  lease lapses the attempt is failed and retried per the task's policy.

Execution itself is at-least-once (a crash after the side effect but before
the result commits re-runs the attempt), so handlers receive a stable
idempotency key.

Callers own the transaction: each function runs inside one.
"""

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID

from nexus.application.interfaces.outbox_writer import OutboxWriter
from nexus.application.interfaces.task_store import TaskRecord, TaskStore
from nexus.application.use_cases.task_events import emit_lifecycle_event, request_dispatch
from nexus.domain.task_lifecycle import (
    FailureDecision,
    plan_cancel,
    plan_claim,
    plan_failure,
    plan_success,
)
from nexus.domain.task_readiness import is_dependency_satisfied, promote_to_ready
from nexus.domain.task_state import TERMINAL_STATES, TaskState

# Re-deliveries that must wait (not_before, a live lease) are deferred with
# the queue's own retry delay, capped so a message never outlives its
# retention; the reconciler covers anything longer.
MAX_DEFER = timedelta(hours=1)
_MIN_DEFER = timedelta(seconds=1)


class ClaimOutcome(StrEnum):
    CLAIMED = "claimed"
    NOT_FOUND = "not_found"
    STALE = "stale"
    TERMINAL = "terminal"
    WAITING_FOR_DEPENDENCIES = "waiting_for_dependencies"
    DEFERRED = "deferred"
    LEASE_EXPIRED = "lease_expired"


@dataclass(frozen=True, slots=True)
class ClaimResult:
    outcome: ClaimOutcome
    task: TaskRecord | None = None
    retry_after: timedelta | None = None


def _defer(until: datetime, now: datetime) -> timedelta:
    return min(max(until - now, _MIN_DEFER), MAX_DEFER)


async def _completed_ids(store: TaskStore, record: TaskRecord) -> frozenset[UUID]:
    deps = await store.get_many(sorted(record.depends_on, key=str))
    return frozenset(d.task_id for d in deps if d.state is TaskState.SUCCEEDED)


async def claim_task(
    store: TaskStore,
    outbox: OutboxWriter,
    *,
    task_id: UUID,
    attempt: int,
    lease_grace: timedelta,
) -> ClaimResult:
    record = await store.get_for_update(task_id)
    if record is None:
        return ClaimResult(ClaimOutcome.NOT_FOUND)
    if record.state in TERMINAL_STATES:
        return ClaimResult(ClaimOutcome.TERMINAL, record)
    if attempt != record.attempt:
        return ClaimResult(ClaimOutcome.STALE, record)

    now = await store.now()

    if record.state is TaskState.PENDING:
        completed = await _completed_ids(store, record)
        if not is_dependency_satisfied(record.to_scheduled_task(), completed):
            # A dependency's completion re-dispatches this task.
            return ClaimResult(ClaimOutcome.WAITING_FOR_DEPENDENCIES, record)
        if record.not_before is not None and record.not_before > now:
            return ClaimResult(ClaimOutcome.DEFERRED, record, _defer(record.not_before, now))
        promoted, promotion = promote_to_ready(
            record.to_scheduled_task(), now=now, completed_task_ids=completed
        )
        record = record.with_instance(promoted.instance, ready_since=now)
        await store.append_transitions(task_id, record.attempt, [promotion])

    if record.state is TaskState.READY:
        not_before = record.not_before
        if not_before is not None and not_before > now:
            record = await store.save(record)
            return ClaimResult(ClaimOutcome.DEFERRED, record, _defer(not_before, now))
        plan = plan_claim(
            record.instance,
            now=now,
            timeout=timedelta(seconds=record.timeout_seconds),
            lease_grace=lease_grace,
        )
        record = record.with_instance(
            plan.instance, started_at=now, lease_expires_at=plan.lease_expires_at, last_error=None
        )
        await store.append_transitions(task_id, record.attempt, plan.records)
        record = await store.save(record)
        return ClaimResult(ClaimOutcome.CLAIMED, record)

    if record.state is TaskState.RUNNING:
        if record.lease_expires_at is not None and record.lease_expires_at > now:
            # Another worker may still be executing this attempt.
            return ClaimResult(ClaimOutcome.DEFERRED, record, _defer(record.lease_expires_at, now))
        record = await fail_attempt(
            store, outbox, record, now=now, reason="lease expired: worker lost or timed out"
        )
        return ClaimResult(ClaimOutcome.LEASE_EXPIRED, record)

    return ClaimResult(ClaimOutcome.STALE, record)


async def record_success(
    store: TaskStore, outbox: OutboxWriter, *, task_id: UUID, attempt: int, result: Any
) -> TaskRecord | None:
    """Returns None when the attempt is no longer the live one (cancelled or
    superseded); the result is then discarded."""
    record = await store.get_for_update(task_id)
    if record is None or record.state is not TaskState.RUNNING or record.attempt != attempt:
        return None
    now = await store.now()
    instance, transition = plan_success(record.instance, now=now)
    record = record.with_instance(instance, result=result, finished_at=now, lease_expires_at=None)
    await store.append_transitions(task_id, attempt, [transition])
    record = await store.save(record)
    await emit_lifecycle_event(outbox, record, now=now)
    await _promote_dependents(store, outbox, record, now=now)
    return record


async def record_failure(
    store: TaskStore,
    outbox: OutboxWriter,
    *,
    task_id: UUID,
    attempt: int,
    error: str,
    permanent: bool = False,
) -> TaskRecord | None:
    record = await store.get_for_update(task_id)
    if record is None or record.state is not TaskState.RUNNING or record.attempt != attempt:
        return None
    now = await store.now()
    return await fail_attempt(store, outbox, record, now=now, reason=error, permanent=permanent)


async def cancel_task(
    store: TaskStore, outbox: OutboxWriter, *, task_id: UUID, tenant_id: str, reason: str
) -> TaskRecord | None:
    """Returns None if the task does not exist for this tenant. Raises
    InvalidTaskTransitionError if it is already terminal. A RUNNING task is
    marked CANCELLED immediately; its worker's result is then discarded
    (cooperative interruption of the handler is not attempted)."""
    record = await store.get_for_update(task_id)
    if record is None or record.tenant_id != tenant_id:
        return None
    now = await store.now()
    instance, transition = plan_cancel(record.instance, now=now, reason=reason)
    record = record.with_instance(
        instance, finished_at=now, lease_expires_at=None, last_error=reason
    )
    await store.append_transitions(task_id, record.attempt, [transition])
    record = await store.save(record)
    await emit_lifecycle_event(outbox, record, now=now, detail=reason)
    await cancel_dependents(store, outbox, record, now=now)
    return record


async def fail_attempt(
    store: TaskStore,
    outbox: OutboxWriter,
    record: TaskRecord,
    *,
    now: datetime,
    reason: str,
    permanent: bool = False,
) -> TaskRecord:
    plan = plan_failure(
        record.instance,
        attempt=record.attempt,
        max_attempts=record.attempt if permanent else record.max_attempts,
        initial_backoff_seconds=record.initial_backoff_seconds,
        max_backoff_seconds=record.max_backoff_seconds,
        now=now,
        reason=reason,
    )
    failed_attempt = record.attempt
    await store.append_transitions(record.task_id, failed_attempt, plan.records)

    if plan.decision is FailureDecision.RETRY and plan.next_attempt is not None:
        record = record.with_instance(
            plan.instance,
            attempt=plan.next_attempt,
            last_error=reason,
            lease_expires_at=None,
            not_before=plan.retry_not_before,
            ready_since=now,
        )
        record = await request_dispatch(
            outbox, record, now=now, deliver_after=plan.retry_not_before
        )
        return await store.save(record)

    record = record.with_instance(
        plan.instance, last_error=reason, lease_expires_at=None, finished_at=now
    )
    record = await store.save(record)
    await emit_lifecycle_event(outbox, record, now=now, detail=reason)
    await cancel_dependents(store, outbox, record, now=now)
    return record


async def _promote_dependents(
    store: TaskStore, outbox: OutboxWriter, completed: TaskRecord, *, now: datetime
) -> None:
    """Dependents are row-locked before their other dependencies are read, so
    two dependencies completing concurrently serialize here and the second
    one always observes the first one's commit."""
    for dependent in await store.pending_dependents_for_update(completed.task_id):
        done = await _completed_ids(store, dependent)
        if not is_dependency_satisfied(dependent.to_scheduled_task(), done):
            continue
        if dependent.not_before is not None and dependent.not_before > now:
            # The claim promotes it once not_before passes.
            updated = await request_dispatch(
                outbox, dependent, now=now, deliver_after=dependent.not_before
            )
            await store.save(updated)
            continue
        promoted, promotion = promote_to_ready(
            dependent.to_scheduled_task(), now=now, completed_task_ids=done
        )
        updated = dependent.with_instance(promoted.instance, ready_since=now)
        await store.append_transitions(updated.task_id, updated.attempt, [promotion])
        updated = await request_dispatch(outbox, updated, now=now)
        await store.save(updated)


async def cancel_dependents(
    store: TaskStore, outbox: OutboxWriter, root: TaskRecord, *, now: datetime
) -> int:
    """Cancels every PENDING task that (transitively) depends on `root`,
    which can no longer succeed. Returns how many were cancelled."""
    cancelled = 0
    frontier: deque[TaskRecord] = deque([root])
    while frontier:
        upstream = frontier.popleft()
        for dependent in await store.pending_dependents_for_update(upstream.task_id):
            reason = f"dependency {upstream.task_id} ended in {upstream.state.value}"
            instance, transition = plan_cancel(dependent.instance, now=now, reason=reason)
            updated = dependent.with_instance(instance, finished_at=now, last_error=reason)
            await store.append_transitions(updated.task_id, updated.attempt, [transition])
            updated = await store.save(updated)
            await emit_lifecycle_event(outbox, updated, now=now, detail=reason)
            frontier.append(updated)
            cancelled += 1
    return cancelled
