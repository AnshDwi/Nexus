from collections.abc import Iterator, Sequence
from datetime import UTC, datetime

from nexus.domain.scheduled_task import ScheduledTask
from nexus.domain.task_readiness import is_eligible_for_ready, promote_to_ready
from nexus.domain.task_state import TaskState, TaskTransitionRecord

_EPOCH = datetime.min.replace(tzinfo=UTC)


class ReadyQueue:
    """Deterministic ordering of tasks already in READY state.

    Base order (pre-EDF): by ready_since ascending (FIFO), then task_id as a
    total, stable tiebreaker so ordering never depends on input/insertion
    order. A task with no known ready_since sorts as if it had been ready
    since the epoch -- conservative (never starved by an unknown timestamp),
    documented rather than accidental.

    EDF (Step 2) and fair-share (Step 3) are separate, composable ordering
    policies layered on top of what this class hands them -- this class
    does not change to accommodate them.
    """

    def __init__(self, tasks: Sequence[ScheduledTask]) -> None:
        not_ready = [task for task in tasks if task.state != TaskState.READY]
        if not_ready:
            states = sorted({task.state.value for task in not_ready})
            raise ValueError(f"ReadyQueue only accepts READY tasks; got state(s): {states}")
        self._tasks: tuple[ScheduledTask, ...] = tuple(sorted(tasks, key=self._sort_key))

    @staticmethod
    def _sort_key(task: ScheduledTask) -> tuple[datetime, str]:
        ready_since = task.ready_since if task.ready_since is not None else _EPOCH
        return (ready_since, str(task.task_id))

    def __iter__(self) -> Iterator[ScheduledTask]:
        return iter(self._tasks)

    def __len__(self) -> int:
        return len(self._tasks)

    def as_tuple(self) -> tuple[ScheduledTask, ...]:
        return self._tasks


def build_ready_queue(
    tasks: Sequence[ScheduledTask], *, now: datetime
) -> tuple[ReadyQueue, tuple[TaskTransitionRecord, ...]]:
    """Populates the ready queue from a raw pool of tasks in any state:
    tasks already READY are included as-is; PENDING tasks whose dependencies
    and not_before are satisfied are promoted to READY (producing a
    transition record each); everything else is excluded. The result always
    satisfies ReadyQueue's invariant that every member is genuinely READY --
    there is no "logically ready but still PENDING" middle state.
    """
    completed_task_ids = frozenset(
        task.task_id for task in tasks if task.state == TaskState.SUCCEEDED
    )

    ready_tasks: list[ScheduledTask] = []
    records: list[TaskTransitionRecord] = []

    for task in tasks:
        if task.state == TaskState.READY:
            ready_tasks.append(task)
        elif task.state == TaskState.PENDING and is_eligible_for_ready(
            task, now=now, completed_task_ids=completed_task_ids
        ):
            promoted, record = promote_to_ready(
                task, now=now, completed_task_ids=completed_task_ids
            )
            ready_tasks.append(promoted)
            records.append(record)

    return ReadyQueue(ready_tasks), tuple(records)
