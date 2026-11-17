"""Task submission: admission, dependency validation, persistence and the
first dispatch request -- all in the caller's single transaction, so a task
row exists if and only if its dispatch request (outbox row) does."""

from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from nexus.application.interfaces.outbox_writer import OutboxWriter
from nexus.application.interfaces.task_handler import TaskHandlerRegistry
from nexus.application.interfaces.task_store import TaskRecord, TaskStore
from nexus.application.use_cases.admission_controller import AdmissionController
from nexus.application.use_cases.task_events import emit_lifecycle_event, request_dispatch
from nexus.domain.admission import AdmissionDecision, AdmissionOutcome, RuleResult
from nexus.domain.task_lifecycle import blocks_dependents
from nexus.domain.task_readiness import is_dependency_satisfied, promote_to_ready
from nexus.domain.task_state import TaskInstance, TaskState, TaskTransitionRecord
from nexus.domain.task_submission import RetryPolicy, TaskResourceRequirements, TaskSubmission


@dataclass(frozen=True, slots=True)
class SubmitTaskCommand:
    tenant_id: str
    task_type: str
    payload: dict[str, Any]
    timeout_seconds: int
    retry_policy: RetryPolicy | None
    job_id: UUID | None = None
    deadline: datetime | None = None
    not_before: datetime | None = None
    estimated_duration: timedelta | None = None
    fair_share_weight: float | None = None
    depends_on: Sequence[UUID] = field(default_factory=tuple)
    resource_requirements: TaskResourceRequirements | None = None
    idempotency_key: str | None = None


@dataclass(frozen=True, slots=True)
class SubmitTaskResult:
    task: TaskRecord
    created: bool
    decision: AdmissionDecision | None


def _rejection(task_id: UUID, now: datetime, rule: str, detail: str) -> AdmissionDecision:
    return AdmissionDecision(
        task_id=task_id,
        outcome=AdmissionOutcome.REJECTED,
        reason=f"{rule}: {detail}",
        evaluated_at=now,
        validation_results=(RuleResult(rule_name=rule, passed=False, detail=detail),),
        schedulability_result=None,
    )


async def _dependency_rejection(
    store: TaskStore, command: SubmitTaskCommand, task_id: UUID, now: datetime
) -> tuple[AdmissionDecision | None, frozenset[UUID]]:
    """Dependencies must already exist in the same tenant. Because a task can
    only reference tasks created before it, the dependency graph is acyclic
    by construction."""
    wanted = frozenset(command.depends_on)
    if not wanted:
        return None, frozenset()
    found = {d.task_id: d for d in await store.get_many(sorted(wanted, key=str))}
    missing = sorted(
        str(t) for t in wanted if t not in found or found[t].tenant_id != command.tenant_id
    )
    if missing:
        return _rejection(
            task_id, now, "dependencies", f"unknown task id(s): {missing}"
        ), frozenset()
    blocked = sorted(str(t) for t, d in found.items() if blocks_dependents(d.state))
    if blocked:
        return (
            _rejection(task_id, now, "dependencies", f"dependency can never succeed: {blocked}"),
            frozenset(),
        )
    return None, frozenset(t for t, d in found.items() if d.state is TaskState.SUCCEEDED)


async def submit_task(
    store: TaskStore,
    outbox: OutboxWriter,
    handlers: TaskHandlerRegistry,
    admission: AdmissionController,
    command: SubmitTaskCommand,
) -> SubmitTaskResult:
    task_id = uuid4()
    now = await store.now()

    decision: AdmissionDecision | None = None
    if handlers.get(command.task_type) is None:
        decision = _rejection(
            task_id,
            now,
            "task_type",
            f"unknown task_type '{command.task_type}'; available: {sorted(handlers.task_types())}",
        )
    completed: frozenset[UUID] = frozenset()
    if decision is None:
        decision, completed = await _dependency_rejection(store, command, task_id, now)
    if decision is None:
        decision = admission.evaluate(
            TaskSubmission(
                task_id=task_id,
                job_id=command.job_id or task_id,
                tenant_id=command.tenant_id,
                deadline=command.deadline,
                not_before=command.not_before,
                estimated_duration=command.estimated_duration,
                retry_policy=command.retry_policy,
                fair_share_weight=command.fair_share_weight,
                resource_requirements=command.resource_requirements,
            ),
            now=now,
        )

    policy = command.retry_policy or RetryPolicy()
    record = TaskRecord(
        task_id=task_id,
        tenant_id=command.tenant_id,
        job_id=command.job_id or task_id,
        task_type=command.task_type,
        payload=command.payload,
        state=TaskState.AWAITING_ADMISSION,
        attempt=1,
        max_attempts=max(policy.max_attempts or 1, 1),
        initial_backoff_seconds=max(policy.initial_backoff_seconds or 0.0, 0.0),
        max_backoff_seconds=max(policy.max_backoff_seconds or 0.0, 0.0),
        timeout_seconds=command.timeout_seconds,
        fair_share_weight=command.fair_share_weight or 1.0,
        depends_on=frozenset(command.depends_on),
        deadline=command.deadline,
        not_before=command.not_before,
        estimated_duration_seconds=(
            command.estimated_duration.total_seconds() if command.estimated_duration else None
        ),
        resource_requirements=(
            asdict(command.resource_requirements) if command.resource_requirements else None
        ),
        idempotency_key=command.idempotency_key,
        admission_reason=decision.reason,
        created_at=now,
        updated_at=now,
    )

    transitions: list[TaskTransitionRecord] = []
    instance: TaskInstance = record.instance
    dispatch_after: datetime | None = None
    dispatch = False

    if decision.outcome is AdmissionOutcome.REJECTED:
        instance, rejected = instance.transition_to(
            TaskState.REJECTED, occurred_at=now, reason=decision.reason
        )
        transitions.append(rejected)
        record = record.with_instance(instance, finished_at=now, last_error=decision.reason)
    else:
        instance, admitted = instance.transition_to(
            TaskState.PENDING, occurred_at=now, reason="admitted"
        )
        transitions.append(admitted)
        record = record.with_instance(instance)
        scheduled = record.to_scheduled_task()
        if is_dependency_satisfied(scheduled, completed):
            if record.not_before is not None and record.not_before > now:
                # Stays PENDING; the delayed message's claim promotes it.
                dispatch, dispatch_after = True, record.not_before
            else:
                promoted, promotion = promote_to_ready(
                    scheduled, now=now, completed_task_ids=completed
                )
                transitions.append(promotion)
                record = record.with_instance(promoted.instance, ready_since=now)
                dispatch = True
        # Otherwise: PENDING until the last dependency succeeds.

    stored, created = await store.insert_or_get_existing(record)
    if not created:
        return SubmitTaskResult(task=stored, created=False, decision=None)

    await store.append_transitions(stored.task_id, stored.attempt, transitions)
    if dispatch:
        stored = await request_dispatch(outbox, stored, now=now, deliver_after=dispatch_after)
        stored = await store.save(stored)
    if stored.state is TaskState.REJECTED:
        await emit_lifecycle_event(outbox, stored, now=now, detail=decision.reason)
    return SubmitTaskResult(task=stored, created=True, decision=decision)
