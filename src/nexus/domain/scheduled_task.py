from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from nexus.domain.task_state import TaskInstance, TaskState
from nexus.domain.task_submission import TaskResourceRequirements


@dataclass(frozen=True, slots=True)
class ScheduledTask:
    """Everything the scheduler needs to know about a task to make a
    dispatch decision.

    Composes TaskInstance (state-machine correctness, Phase 3) with the
    scheduling metadata TaskInstance deliberately does not carry -- this is
    the "richer task aggregate" flagged as an open point in Phase 3's
    report. Composition, not inheritance or added fields on TaskInstance
    itself: TaskInstance stays single-purpose (valid transitions only).
    """

    instance: TaskInstance
    tenant_id: str
    deadline: datetime | None
    not_before: datetime | None
    fair_share_weight: float
    depends_on: frozenset[UUID]
    resource_requirements: TaskResourceRequirements | None
    ready_since: datetime | None

    @property
    def task_id(self) -> UUID:
        return self.instance.task_id

    @property
    def state(self) -> TaskState:
        return self.instance.state
