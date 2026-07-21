import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from nexus.application.use_cases.admission_controller import (
    DEFAULT_ADMISSION_RULES,
    AdmissionController,
)
from nexus.domain.admission import AdmissionOutcome, RuleResult
from nexus.domain.task_submission import RetryPolicy, TaskSubmission

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def _valid_submission(**overrides) -> TaskSubmission:
    defaults = {
        "task_id": uuid4(),
        "job_id": uuid4(),
        "tenant_id": "acme",
        "deadline": NOW + timedelta(hours=1),
        "estimated_duration": timedelta(minutes=5),
        "retry_policy": RetryPolicy(
            max_attempts=3, initial_backoff_seconds=1.0, max_backoff_seconds=60.0
        ),
        "fair_share_weight": 1.0,
    }
    defaults.update(overrides)
    return TaskSubmission(**defaults)


def test_fully_valid_submission_is_accepted():
    controller = AdmissionController()

    decision = controller.evaluate(_valid_submission(), now=NOW)

    assert decision.outcome == AdmissionOutcome.ACCEPTED
    assert decision.schedulability_result is not None
    assert decision.schedulability_result.feasible is True


def test_every_default_rule_is_evaluated_even_when_all_pass():
    """Proves no short-circuiting: 'which rules were evaluated' should mean
    all of them, not just until the first pass."""
    controller = AdmissionController()

    decision = controller.evaluate(_valid_submission(), now=NOW)

    assert len(decision.validation_results) == len(DEFAULT_ADMISSION_RULES)
    assert all(result.passed for result in decision.validation_results)


def test_single_rule_failure_is_rejected_with_that_rule_named_in_the_reason():
    controller = AdmissionController()

    decision = controller.evaluate(_valid_submission(tenant_id=None), now=NOW)

    assert decision.outcome == AdmissionOutcome.REJECTED
    assert "required_fields" in decision.reason
    failed = [r for r in decision.validation_results if not r.passed]
    assert len(failed) == 1
    assert failed[0].rule_name == "required_fields"


def test_multiple_rule_failures_are_all_reported_but_reason_cites_the_first():
    controller = AdmissionController()
    bad_policy = RetryPolicy(max_attempts=-1)

    decision = controller.evaluate(
        _valid_submission(tenant_id=None, retry_policy=bad_policy), now=NOW
    )

    assert decision.outcome == AdmissionOutcome.REJECTED
    failed_names = {r.rule_name for r in decision.validation_results if not r.passed}
    # required_fields fails (tenant_id missing AND retry_policy present-but-garbage
    # is still "present" for that rule); retry_policy_consistency fails on its own terms.
    assert "required_fields" in failed_names
    assert "retry_policy_consistency" in failed_names
    # DEFAULT_ADMISSION_RULES orders required_fields first.
    assert decision.reason.startswith("required_fields")


def test_all_rules_pass_but_unschedulable_is_rejected():
    controller = AdmissionController()

    decision = controller.evaluate(
        _valid_submission(
            deadline=NOW + timedelta(minutes=1), estimated_duration=timedelta(hours=1)
        ),
        now=NOW,
    )

    assert decision.outcome == AdmissionOutcome.REJECTED
    assert all(result.passed for result in decision.validation_results)
    assert decision.schedulability_result is not None
    assert decision.schedulability_result.feasible is False
    assert "not schedulable" in decision.reason


def test_evaluation_is_deterministic_for_the_same_input():
    controller = AdmissionController()
    submission = _valid_submission()

    first = controller.evaluate(submission, now=NOW)
    second = controller.evaluate(submission, now=NOW)

    assert first == second


class _AlwaysRejectRule:
    name = "always_reject"

    def evaluate(self, submission: TaskSubmission, *, now: datetime) -> RuleResult:
        return RuleResult(rule_name=self.name, passed=False, detail="test rule always rejects")


def test_custom_rule_set_replaces_defaults_entirely():
    """Extensibility: a caller can supply its own rule set and the defaults
    are not silently mixed in."""
    controller = AdmissionController(rules=[_AlwaysRejectRule()])

    decision = controller.evaluate(_valid_submission(), now=NOW)

    assert decision.outcome == AdmissionOutcome.REJECTED
    assert len(decision.validation_results) == 1
    assert decision.validation_results[0].rule_name == "always_reject"


def test_evaluating_one_thousand_submissions_completes_quickly():
    """Sanity check, not a meaningful 'benchmark' in the Phase 1/2 sense --
    this is pure in-process logic with zero I/O, so timing it mainly guards
    against an accidental pathological (e.g. quadratic) implementation."""
    controller = AdmissionController()
    submissions = [_valid_submission() for _ in range(1000)]

    start = time.perf_counter()
    for submission in submissions:
        controller.evaluate(submission, now=NOW)
    elapsed = time.perf_counter() - start

    assert elapsed < 1.0
