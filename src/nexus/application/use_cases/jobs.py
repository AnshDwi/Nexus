"""Job use cases: submission, dispatch confirmation, execution reporting,
cancellation, manual retry and leader-side recovery.

Transaction boundaries belong to the caller (API request, relay iteration,
worker, scheduler tick); each function runs inside exactly one transaction.

Correctness rules enforced here:
* A job becomes QUEUED only in the relay transaction that saw the broker's
  publisher confirm (JobDispatchHook.after_publish).
* A worker may only run the job attempt named in the message, and only once
  per message (inbox). Results are accepted only from the worker that holds
  the live attempt -- `attempt` + `worker_id` act as the fencing token, so a
  worker that was declared dead cannot overwrite the retry's outcome.
* Retries go back through the outbox with `deliver_after`, so RETRYING ->
  QUEUED again happens only after a confirmed publish.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from nexus.application.interfaces.inbox_repository import InboxRepository
from nexus.application.interfaces.job_store import JobStore
from nexus.application.interfaces.outbox_writer import OutboxWriter
from nexus.application.interfaces.task_handler import TaskHandlerRegistry
from nexus.application.use_cases.admission_controller import AdmissionController
from nexus.domain.admission import AdmissionOutcome, RuleResult
from nexus.domain.events import DomainEvent
from nexus.domain.idempotency import IdempotencyKey
from nexus.domain.job import (
    CANCELLABLE_JOB_STATES,
    DISPATCHABLE_JOB_STATES,
    InvalidJobTransitionError,
    Job,
    JobNotRetryableError,
    JobPriority,
    JobState,
)
from nexus.domain.task_submission import RetryPolicy, TaskSubmission

JOB_AGGREGATE = "job"
JOB_DISPATCH_EVENT = "dispatch_requested"
# Outbox hold-back (see PostgresOutboxWriter.add): the relay publishes the
# retry dispatch only once it is due, so RETRYING -> QUEUED happens when the
# broker actually receives the message.
HOLD_UNTIL_KEY = "available_at"
EXECUTOR_INBOX_SCOPE = "job-executor"


class JobRejectedError(Exception):
    def __init__(self, reasons: list[RuleResult]) -> None:
        super().__init__("; ".join(f"{r.rule_name}: {r.detail}" for r in reasons))
        self.reasons = reasons


class JobNotFoundError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class CreateJobCommand:
    tenant_id: str
    name: str
    job_type: str
    payload: dict[str, Any]
    priority: JobPriority
    max_attempts: int
    initial_backoff_seconds: float
    max_backoff_seconds: float
    timeout_seconds: int
    idempotency_key: str | None = None
    correlation_id: str | None = None


def is_job_dispatch(event: DomainEvent) -> bool:
    return event.aggregate_type == JOB_AGGREGATE and event.event_type == JOB_DISPATCH_EVENT


async def request_job_dispatch(
    outbox: OutboxWriter, job: Job, *, now: datetime, deliver_after: datetime | None = None
) -> None:
    metadata: dict[str, Any] = {
        "tenant_id": job.tenant_id,
        "priority": job.priority.value,
        "broker_priority": job.priority.broker_priority,
    }
    if job.correlation_id:
        metadata["correlation_id"] = job.correlation_id
    if deliver_after is not None and deliver_after > now:
        metadata[HOLD_UNTIL_KEY] = deliver_after.isoformat()
    await outbox.add(
        DomainEvent(
            aggregate_id=str(job.job_id),
            aggregate_type=JOB_AGGREGATE,
            event_type=JOB_DISPATCH_EVENT,
            payload={"job_id": str(job.job_id), "attempt": job.attempt},
            occurred_at=now,
            metadata=metadata,
        )
    )


# --- submission ---------------------------------------------------------------


async def create_job(
    store: JobStore,
    outbox: OutboxWriter,
    handlers: TaskHandlerRegistry,
    admission: AdmissionController,
    command: CreateJobCommand,
) -> tuple[Job, bool]:
    """Returns (job, created). With an idempotency key already used by this
    tenant, returns the original job and created=False (no new dispatch)."""
    job_id = uuid4()
    now = await store.now()
    problems: list[RuleResult] = []
    if handlers.get(command.job_type) is None:
        problems.append(
            RuleResult(
                rule_name="job_type",
                passed=False,
                detail=f"unknown job type '{command.job_type}'; "
                f"available: {sorted(handlers.task_types())}",
            )
        )
    decision = admission.evaluate(
        TaskSubmission(
            task_id=job_id,
            job_id=job_id,
            tenant_id=command.tenant_id,
            retry_policy=RetryPolicy(
                max_attempts=command.max_attempts,
                initial_backoff_seconds=command.initial_backoff_seconds,
                max_backoff_seconds=command.max_backoff_seconds,
            ),
        ),
        now=now,
    )
    if decision.outcome is AdmissionOutcome.REJECTED:
        problems.extend(r for r in decision.validation_results if not r.passed)
    if problems:
        raise JobRejectedError(problems)

    job = Job(
        job_id=job_id,
        tenant_id=command.tenant_id,
        name=command.name,
        job_type=command.job_type,
        payload=command.payload,
        priority=command.priority,
        state=JobState.PENDING,
        attempt=1,
        max_attempts=command.max_attempts,
        retry_limit=command.max_attempts,
        initial_backoff_seconds=command.initial_backoff_seconds,
        max_backoff_seconds=command.max_backoff_seconds,
        timeout_seconds=command.timeout_seconds,
        created_at=now,
        updated_at=now,
        idempotency_key=command.idempotency_key,
        correlation_id=command.correlation_id,
    )
    stored, created = await store.insert_or_get_existing(job)
    if not created:
        return stored, False
    await request_job_dispatch(outbox, stored, now=now)
    stored = await store.save(stored, dispatch_requested_at=now)
    return stored, True


# --- dispatch confirmation (outbox relay hook) --------------------------------


class JobDispatchHook:
    """Plugged into the outbox relay. Locks the job row *before* publishing
    so a worker that receives the message immediately blocks on the row
    until this transaction records QUEUED; skips publishing for attempts
    that are no longer dispatchable (cancelled, already running, superseded)."""

    def __init__(self, store: JobStore) -> None:
        self._store = store
        self._job: Job | None = None

    async def before_publish(self, event: DomainEvent) -> bool:
        self._job = None
        if not is_job_dispatch(event):
            return True
        job = await self._store.get_for_update(UUID(event.payload["job_id"]))
        if (
            job is None
            or job.state not in DISPATCHABLE_JOB_STATES
            or job.attempt != int(event.payload["attempt"])
        ):
            return False
        self._job = job
        return True

    async def after_publish(self, event: DomainEvent) -> None:
        job, self._job = self._job, None
        if job is None or job.state is JobState.QUEUED:
            return
        now = await self._store.now()
        queued, transition = job.mark_queued(now=now)
        await self._store.append_transitions(job.job_id, [transition])
        await self._store.save(queued)


# --- execution ------------------------------------------------------------------


class ClaimDecision(StrEnum):
    RUN = "run"
    DUPLICATE = "duplicate"
    STALE = "stale"


async def claim_job(
    store: JobStore,
    inbox: InboxRepository,
    *,
    job_id: UUID,
    attempt: int,
    message_id: str,
    worker_id: UUID,
    lease_grace: timedelta,
) -> tuple[ClaimDecision, Job | None]:
    first_delivery = await inbox.mark_processed_if_new(
        IdempotencyKey(f"{EXECUTOR_INBOX_SCOPE}:{message_id}")
    )
    if not first_delivery:
        return ClaimDecision.DUPLICATE, None
    job = await store.get_for_update(job_id)
    if job is None or job.attempt != attempt or job.state not in DISPATCHABLE_JOB_STATES:
        return ClaimDecision.STALE, job
    now = await store.now()
    running, transitions = job.start(worker_id=worker_id, now=now, lease_grace=lease_grace)
    await store.append_transitions(job_id, transitions)
    running = await store.save(running)
    await store.start_execution(running)
    return ClaimDecision.RUN, running


async def _live_attempt(store: JobStore, job_id: UUID, attempt: int, worker_id: UUID) -> Job | None:
    job = await store.get_for_update(job_id)
    if (
        job is None
        or job.state is not JobState.RUNNING
        or job.attempt != attempt
        or job.worker_id != worker_id
    ):
        return None  # fenced: cancelled, recovered by the scheduler, or superseded
    return job


async def report_success(
    store: JobStore, *, job_id: UUID, attempt: int, worker_id: UUID, result: Any
) -> Job | None:
    job = await _live_attempt(store, job_id, attempt, worker_id)
    if job is None:
        return None
    now = await store.now()
    completed, transition = job.complete(result=result, now=now)
    await store.append_transitions(job_id, [transition])
    completed = await store.save(completed)
    await store.finish_execution(
        completed, attempt=attempt, status="completed", error=None, finished_at=now
    )
    return completed


async def report_failure(
    store: JobStore,
    outbox: OutboxWriter,
    *,
    job_id: UUID,
    attempt: int,
    worker_id: UUID,
    error: str,
    permanent: bool = False,
    execution_status: str = "failed",
) -> Job | None:
    job = await _live_attempt(store, job_id, attempt, worker_id)
    if job is None:
        return None
    return await _fail_running(
        store, outbox, job, error=error, permanent=permanent, execution_status=execution_status
    )


async def _fail_running(
    store: JobStore,
    outbox: OutboxWriter,
    job: Job,
    *,
    error: str,
    permanent: bool,
    execution_status: str,
) -> Job:
    now = await store.now()
    failed_attempt = job.attempt
    updated, transitions = job.fail(error=error, now=now, permanent=permanent)
    await store.append_transitions(job.job_id, transitions)
    if updated.state is JobState.RETRYING:
        await request_job_dispatch(outbox, updated, now=now, deliver_after=updated.next_attempt_at)
        updated = await store.save(updated, dispatch_requested_at=now)
    else:
        updated = await store.save(updated)
    await store.finish_execution(
        updated, attempt=failed_attempt, status=execution_status, error=error, finished_at=now
    )
    return updated


# --- operator actions -------------------------------------------------------------


async def cancel_job(store: JobStore, *, job_id: UUID, tenant_id: str, reason: str) -> Job:
    job = await store.get_for_update(job_id)
    if job is None or job.tenant_id != tenant_id:
        raise JobNotFoundError(str(job_id))
    was_running = job.state is JobState.RUNNING
    if job.state not in CANCELLABLE_JOB_STATES:
        # Let the domain produce the canonical error.
        job.cancel(now=job.updated_at, reason=reason)
    now = await store.now()
    cancelled, transition = job.cancel(now=now, reason=reason)
    await store.append_transitions(job_id, [transition])
    cancelled = await store.save(cancelled)
    if was_running:
        await store.finish_execution(
            cancelled, attempt=job.attempt, status="cancelled", error=reason, finished_at=now
        )
    return cancelled


async def retry_job(store: JobStore, outbox: OutboxWriter, *, job_id: UUID, tenant_id: str) -> Job:
    job = await store.get_for_update(job_id)
    if job is None or job.tenant_id != tenant_id:
        raise JobNotFoundError(str(job_id))
    now = await store.now()
    retrying, transition = job.retry_manually(now=now)
    await store.append_transitions(job_id, [transition])
    await request_job_dispatch(outbox, retrying, now=now)
    return await store.save(retrying, dispatch_requested_at=now)


@dataclass(frozen=True, slots=True)
class BulkRetryOutcome:
    retried: list[Job]
    skipped: list[tuple[UUID, str, str]]  # (job_id, code, message)


async def retry_jobs(
    store: JobStore, outbox: OutboxWriter, *, job_ids: list[UUID], tenant_id: str
) -> BulkRetryOutcome:
    """Retry several final failures in one transaction. A job that cannot be
    retried is reported and skipped; it does not undo the others. The checks
    in `retry_job` run before any write, so a skipped job leaves no trace."""
    retried: list[Job] = []
    skipped: list[tuple[UUID, str, str]] = []
    for job_id in dict.fromkeys(job_ids):  # de-duplicate, keep order
        try:
            retried.append(await retry_job(store, outbox, job_id=job_id, tenant_id=tenant_id))
        except JobNotFoundError:
            skipped.append((job_id, "not_found", "job not found"))
        except (JobNotRetryableError, InvalidJobTransitionError) as exc:
            skipped.append((job_id, "not_retryable", str(exc)))
    return BulkRetryOutcome(retried=retried, skipped=skipped)


# --- leader-side recovery -----------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RecoveryReport:
    lost_on_dead_workers: int = 0
    expired_leases: int = 0
    redispatched: int = 0

    @property
    def total(self) -> int:
        return self.lost_on_dead_workers + self.expired_leases + self.redispatched


async def recover_jobs(
    store: JobStore,
    outbox: OutboxWriter,
    *,
    dead_worker_ids: Sequence[UUID],
    redispatch_after: timedelta,
    batch_size: int,
) -> RecoveryReport:
    """Run by the elected scheduler, inside a transaction the caller has
    verified with the current fencing token."""
    now = await store.now()
    lost = 0
    if dead_worker_ids:
        for job in await store.running_on_workers_for_update(dead_worker_ids, batch_size):
            await _fail_running(
                store,
                outbox,
                job,
                error=f"worker {job.worker_id} stopped heartbeating",
                permanent=False,
                execution_status="lost",
            )
            lost += 1

    expired = 0
    for job in await store.expired_leases_for_update(now, batch_size):
        await _fail_running(
            store,
            outbox,
            job,
            error=f"execution lease expired (timeout {job.timeout_seconds}s)",
            permanent=False,
            execution_status="timed_out",
        )
        expired += 1

    redispatched = 0
    for job in await store.stale_dispatch_for_update(now - redispatch_after, batch_size):
        await request_job_dispatch(outbox, job, now=now)
        await store.save(job, dispatch_requested_at=now)
        redispatched += 1

    return RecoveryReport(
        lost_on_dead_workers=lost, expired_leases=expired, redispatched=redispatched
    )
