from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID

from nexus.domain.task_submission import TaskSubmission


class AdmissionOutcome(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class RuleResult:
    rule_name: str
    passed: bool
    detail: str


@dataclass(frozen=True, slots=True)
class SchedulabilityResult:
    """A single-task feasibility pre-check only: is there numerically enough
    lead time before the deadline for the declared estimated duration.

    This is deliberately NOT the full multi-task Liu & Layland
    utilization-bound test -- that requires knowledge of the whole ready set
    (which tasks are contending for the same workers) and belongs to the EDF
    scheduler in a later phase. A task can pass this check and still miss
    its deadline once it contends with other work; this only rules out the
    cases that are infeasible in isolation.
    """

    feasible: bool
    detail: str


@dataclass(frozen=True, slots=True)
class AdmissionDecision:
    task_id: UUID
    outcome: AdmissionOutcome
    reason: str
    evaluated_at: datetime
    validation_results: tuple[RuleResult, ...]
    schedulability_result: SchedulabilityResult | None


def evaluate_schedulability(submission: TaskSubmission, *, now: datetime) -> SchedulabilityResult:
    if submission.deadline is None or submission.estimated_duration is None:
        return SchedulabilityResult(
            feasible=True, detail="no deadline/estimated_duration provided; unconstrained"
        )

    earliest_start = submission.not_before if submission.not_before is not None else now
    if earliest_start > submission.deadline:
        # DeadlineValidityRule already reports this ordering problem as a
        # validation failure; schedulability reports it too so this result
        # is never misleadingly "feasible" when the ordering itself is broken.
        return SchedulabilityResult(
            feasible=False, detail="earliest possible start is after the deadline"
        )

    available = submission.deadline - earliest_start
    if available >= submission.estimated_duration:
        return SchedulabilityResult(
            feasible=True,
            detail=f"available lead time {available} >= estimated duration "
            f"{submission.estimated_duration}",
        )
    return SchedulabilityResult(
        feasible=False,
        detail=f"available lead time {available} is less than estimated duration "
        f"{submission.estimated_duration}",
    )
