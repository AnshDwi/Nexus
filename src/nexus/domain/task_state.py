from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Self
from uuid import UUID


class TaskState(StrEnum):
    AWAITING_ADMISSION = "awaiting_admission"
    PENDING = "pending"
    READY = "ready"
    DISPATCHED = "dispatched"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    PERMANENTLY_FAILED = "permanently_failed"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


TERMINAL_STATES: frozenset[TaskState] = frozenset(
    {TaskState.SUCCEEDED, TaskState.PERMANENTLY_FAILED, TaskState.CANCELLED, TaskState.REJECTED}
)

# The single source of truth for the task lifecycle. Every legal transition is
# an entry here; nothing outside this table (and TaskInstance.transition_to,
# which is the only thing permitted to consult it) should ever branch on task
# state. Terminal states map to an empty set, which is what makes them
# terminal -- immutability isn't a separate rule, it falls out of the table.
ALLOWED_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.AWAITING_ADMISSION: frozenset(
        {TaskState.PENDING, TaskState.REJECTED, TaskState.CANCELLED}
    ),
    TaskState.PENDING: frozenset({TaskState.READY, TaskState.CANCELLED}),
    TaskState.READY: frozenset({TaskState.DISPATCHED, TaskState.CANCELLED}),
    TaskState.DISPATCHED: frozenset({TaskState.RUNNING, TaskState.READY, TaskState.CANCELLED}),
    TaskState.RUNNING: frozenset({TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}),
    TaskState.FAILED: frozenset(
        {TaskState.READY, TaskState.PERMANENTLY_FAILED, TaskState.CANCELLED}
    ),
    TaskState.SUCCEEDED: frozenset(),
    TaskState.PERMANENTLY_FAILED: frozenset(),
    TaskState.CANCELLED: frozenset(),
    TaskState.REJECTED: frozenset(),
}


def is_valid_transition(from_state: TaskState, to_state: TaskState) -> bool:
    return to_state in ALLOWED_TRANSITIONS[from_state]


class InvalidTaskTransitionError(Exception):
    def __init__(self, *, from_state: TaskState, to_state: TaskState) -> None:
        self.from_state = from_state
        self.to_state = to_state
        super().__init__(f"Cannot transition task from '{from_state.value}' to '{to_state.value}'")


@dataclass(frozen=True, slots=True)
class TaskTransitionRecord:
    """What a persistence layer appends to an audit/replay log -- shaped to
    match the task_instance_transitions table described in the Act 2
    replay design, so nothing needs to change when that table is built."""

    from_state: TaskState
    to_state: TaskState
    occurred_at: datetime
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class TaskInstance:
    """Minimal state-machine carrier: a task's identity and current state.

    Deliberately does not carry scheduling metadata (deadline, tenant, DAG
    dependencies) -- those belong to whichever later phase defines the full
    task aggregate around this state machine. This type's only job is
    correct, centrally-enforced state transitions.
    """

    task_id: UUID
    state: TaskState

    def transition_to(
        self, new_state: TaskState, *, occurred_at: datetime, reason: str | None = None
    ) -> tuple[Self, TaskTransitionRecord]:
        """`occurred_at` is a required parameter, not self-generated, so this
        stays deterministic and testable without monkeypatching time -- and
        is already compatible with a future Clock-port-driven simulation
        mode without any change here."""
        if not is_valid_transition(self.state, new_state):
            raise InvalidTaskTransitionError(from_state=self.state, to_state=new_state)
        record = TaskTransitionRecord(
            from_state=self.state, to_state=new_state, occurred_at=occurred_at, reason=reason
        )
        return type(self)(task_id=self.task_id, state=new_state), record
