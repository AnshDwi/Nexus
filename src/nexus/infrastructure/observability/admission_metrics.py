import logging

from nexus.domain.admission import AdmissionDecision, AdmissionOutcome
from nexus.infrastructure.observability.metrics import (
    scheduler_admission_decisions_total,
    scheduler_admission_evaluation_seconds,
)

logger = logging.getLogger("nexus.scheduler.admission")


def record_admission_decision(
    decision: AdmissionDecision, *, evaluation_duration_seconds: float | None = None
) -> None:
    """Deliberately separate from AdmissionController.evaluate(), which stays
    pure and side-effect-free. A future caller -- whichever phase actually
    receives task submissions -- invokes this after the fact. This is the
    same "core algorithm never touches observability directly" principle
    already applied to the leader lease and worker registry: an infra-layer
    adapter instruments the outcome, the pure logic doesn't know it exists.
    """
    failed_rule = next(
        (result.rule_name for result in decision.validation_results if not result.passed),
        None,
    )
    if failed_rule is None and decision.schedulability_result is not None:
        failed_rule = "schedulability" if not decision.schedulability_result.feasible else "none"
    elif failed_rule is None:
        failed_rule = "none"

    scheduler_admission_decisions_total.labels(
        outcome=decision.outcome.value, failed_rule=failed_rule
    ).inc()
    if evaluation_duration_seconds is not None:
        scheduler_admission_evaluation_seconds.observe(evaluation_duration_seconds)

    log_fields = {
        "task_id": str(decision.task_id),
        "outcome": decision.outcome.value,
        "reason": decision.reason,
    }
    if decision.outcome is AdmissionOutcome.ACCEPTED:
        logger.info("task admitted", extra=log_fields)
    else:
        logger.warning("task admission rejected", extra=log_fields)
