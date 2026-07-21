from datetime import datetime
from typing import Protocol

from nexus.domain.admission import RuleResult
from nexus.domain.task_submission import TaskSubmission


class AdmissionRule(Protocol):
    """Extension point for admission validation. A new validation concern is
    a new implementation of this Protocol appended to the rule set --
    AdmissionController itself never needs to change (Open/Closed)."""

    name: str

    def evaluate(self, submission: TaskSubmission, *, now: datetime) -> RuleResult: ...
