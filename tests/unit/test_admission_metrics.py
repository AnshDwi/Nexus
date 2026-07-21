from datetime import UTC, datetime
from uuid import uuid4

from nexus.domain.admission import AdmissionDecision, AdmissionOutcome, SchedulabilityResult
from nexus.domain.admission import RuleResult as DomainRuleResult
from nexus.infrastructure.observability.admission_metrics import record_admission_decision
from nexus.infrastructure.observability.metrics import scheduler_admission_decisions_total

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _decision(*, outcome: AdmissionOutcome, failed_rule_name: str | None) -> AdmissionDecision:
    if failed_rule_name is None:
        results = (DomainRuleResult(rule_name="required_fields", passed=True, detail="ok"),)
        schedulability = SchedulabilityResult(feasible=True, detail="ok")
    else:
        results = (DomainRuleResult(rule_name=failed_rule_name, passed=False, detail="bad"),)
        schedulability = SchedulabilityResult(feasible=True, detail="ok")
    return AdmissionDecision(
        task_id=uuid4(),
        outcome=outcome,
        reason="test",
        evaluated_at=NOW,
        validation_results=results,
        schedulability_result=schedulability,
    )


def test_record_admission_decision_increments_accepted_counter():
    before = scheduler_admission_decisions_total.labels(
        outcome="accepted", failed_rule="none"
    )._value.get()

    record_admission_decision(_decision(outcome=AdmissionOutcome.ACCEPTED, failed_rule_name=None))

    after = scheduler_admission_decisions_total.labels(
        outcome="accepted", failed_rule="none"
    )._value.get()
    assert after == before + 1


def test_record_admission_decision_labels_by_failed_rule_name():
    before = scheduler_admission_decisions_total.labels(
        outcome="rejected", failed_rule="required_fields"
    )._value.get()

    record_admission_decision(
        _decision(outcome=AdmissionOutcome.REJECTED, failed_rule_name="required_fields")
    )

    after = scheduler_admission_decisions_total.labels(
        outcome="rejected", failed_rule="required_fields"
    )._value.get()
    assert after == before + 1


def test_record_admission_decision_does_not_raise_without_duration():
    record_admission_decision(_decision(outcome=AdmissionOutcome.ACCEPTED, failed_rule_name=None))
