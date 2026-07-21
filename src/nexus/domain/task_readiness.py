import dataclasses
from datetime import datetime
from uuid import UUID

from nexus.domain.scheduled_task import ScheduledTask
from nexus.domain.task_state import TaskState, TaskTransitionRecord


class TaskNotEligibleError(Exception):
    def __init__(self, *, task_id: UUID, reason: str) -> None:
        self.task_id = task_id
        self.reason = reason
        super().__init__(f"Task {task_id} is not eligible for READY: {reason}")


def is_dependency_satisfied(task: ScheduledTask, completed_task_ids: frozenset[UUID]) -> bool:
    return task.depends_on <= completed_task_ids


def _ineligibility_reason(
    task: ScheduledTask, *, now: datetime, completed_task_ids: frozenset[UUID]
) -> str | None:
    """Single source of truth for readiness, shared by is_eligible_for_ready
    (a cheap bool check) and promote_to_ready (which needs to explain a
    rejection) -- one place to get this logic right, not two."""
    if task.state != TaskState.PENDING:
        return f"state is '{task.state.value}', not pending"

    if not is_dependency_satisfied(task, completed_task_ids):
        unmet = sorted(str(t) for t in task.depends_on - completed_task_ids)
        return f"unmet dependencies: {unmet}"

    if task.not_before is not None and task.not_before > now:
        return f"not_before ({task.not_before.isoformat()}) is in the future"

    return None


def is_eligible_for_ready(
    task: ScheduledTask, *, now: datetime, completed_task_ids: frozenset[UUID]
) -> bool:
    return _ineligibility_reason(task, now=now, completed_task_ids=completed_task_ids) is None


def promote_to_ready(
    task: ScheduledTask, *, now: datetime, completed_task_ids: frozenset[UUID]
) -> tuple[ScheduledTask, TaskTransitionRecord]:
    reason = _ineligibility_reason(task, now=now, completed_task_ids=completed_task_ids)
    if reason is not None:
        raise TaskNotEligibleError(task_id=task.task_id, reason=reason)

    new_instance, record = task.instance.transition_to(
        TaskState.READY, occurred_at=now, reason="dependencies satisfied"
    )
    new_task = dataclasses.replace(task, instance=new_instance, ready_since=now)
    return new_task, record
