from collections.abc import Sequence
from datetime import datetime

from nexus.application.interfaces.admission_rule import AdmissionRule
from nexus.domain.admission import AdmissionDecision, AdmissionOutcome, evaluate_schedulability
from nexus.domain.admission_rules import (
    DeadlineValidityRule,
    FairShareMetadataRule,
    RequiredFieldsRule,
    ResourceRequirementsRule,
    RetryPolicyConsistencyRule,
)
from nexus.domain.task_submission import TaskSubmission

DEFAULT_ADMISSION_RULES: tuple[AdmissionRule, ...] = (
    RequiredFieldsRule(),
    DeadlineValidityRule(),
    RetryPolicyConsistencyRule(),
    FairShareMetadataRule(),
    ResourceRequirementsRule(),
)


class AdmissionController:
    """Answers only "can this task be admitted?" -- never "when should it
    run?" or "which worker?". Those are EDF/fair-share/dispatch concerns for
    a later phase; this evaluates well-formedness and single-task
    feasibility only, and is pure (no I/O, no side effects) so it stays
    trivially testable and deterministic.
    """

    def __init__(self, rules: Sequence[AdmissionRule] = DEFAULT_ADMISSION_RULES) -> None:
        self._rules = tuple(rules)

    def evaluate(self, submission: TaskSubmission, *, now: datetime) -> AdmissionDecision:
        validation_results = tuple(rule.evaluate(submission, now=now) for rule in self._rules)
        schedulability_result = evaluate_schedulability(submission, now=now)

        failed = [result for result in validation_results if not result.passed]
        if failed:
            outcome = AdmissionOutcome.REJECTED
            reason = f"{failed[0].rule_name}: {failed[0].detail}"
        elif not schedulability_result.feasible:
            outcome = AdmissionOutcome.REJECTED
            reason = f"not schedulable: {schedulability_result.detail}"
        else:
            outcome = AdmissionOutcome.ACCEPTED
            reason = "all validation rules passed and the task is schedulable"

        return AdmissionDecision(
            task_id=submission.task_id,
            outcome=outcome,
            reason=reason,
            evaluated_at=now,
            validation_results=validation_results,
            schedulability_result=schedulability_result,
        )
