from datetime import datetime

from nexus.domain.admission import RuleResult
from nexus.domain.task_submission import TaskSubmission

_REQUIREMENT_STRING_FIELDS = ("architecture", "region", "runtime")


class RequiredFieldsRule:
    """Presence only -- whether a present value is itself *valid* is every
    other rule's job. Keeping this single-responsibility avoids duplicate,
    confusing failure messages for the same root cause."""

    name = "required_fields"

    def evaluate(self, submission: TaskSubmission, *, now: datetime) -> RuleResult:
        missing = []
        if not submission.tenant_id:
            missing.append("tenant_id")
        if submission.retry_policy is None:
            missing.append("retry_policy")

        if missing:
            return RuleResult(
                rule_name=self.name,
                passed=False,
                detail=f"missing required field(s): {', '.join(missing)}",
            )
        return RuleResult(rule_name=self.name, passed=True, detail="all required fields present")


class DeadlineValidityRule:
    name = "deadline_validity"

    def evaluate(self, submission: TaskSubmission, *, now: datetime) -> RuleResult:
        if submission.deadline is None:
            return RuleResult(rule_name=self.name, passed=True, detail="no deadline specified")

        if submission.not_before is not None and submission.not_before > submission.deadline:
            return RuleResult(
                rule_name=self.name,
                passed=False,
                detail=f"not_before ({submission.not_before.isoformat()}) is after deadline "
                f"({submission.deadline.isoformat()})",
            )

        earliest_start = submission.not_before if submission.not_before is not None else now
        if submission.deadline < earliest_start:
            return RuleResult(
                rule_name=self.name,
                passed=False,
                detail=f"deadline ({submission.deadline.isoformat()}) is before the earliest "
                f"possible start ({earliest_start.isoformat()})",
            )
        return RuleResult(rule_name=self.name, passed=True, detail="deadline is valid")


class RetryPolicyConsistencyRule:
    name = "retry_policy_consistency"

    def evaluate(self, submission: TaskSubmission, *, now: datetime) -> RuleResult:
        policy = submission.retry_policy
        if policy is None:
            return RuleResult(
                rule_name=self.name, passed=True, detail="no retry policy to validate"
            )

        problems = []
        if policy.max_attempts is None or policy.max_attempts < 1:
            problems.append("max_attempts must be >= 1")
        if policy.initial_backoff_seconds is None or policy.initial_backoff_seconds < 0:
            problems.append("initial_backoff_seconds must be >= 0")
        if policy.max_backoff_seconds is None or policy.max_backoff_seconds < 0:
            problems.append("max_backoff_seconds must be >= 0")
        if (
            policy.initial_backoff_seconds is not None
            and policy.max_backoff_seconds is not None
            and policy.initial_backoff_seconds > policy.max_backoff_seconds
        ):
            problems.append("initial_backoff_seconds must not exceed max_backoff_seconds")

        if problems:
            return RuleResult(rule_name=self.name, passed=False, detail="; ".join(problems))
        return RuleResult(rule_name=self.name, passed=True, detail="retry policy is consistent")


class FairShareMetadataRule:
    name = "fair_share_metadata"

    def evaluate(self, submission: TaskSubmission, *, now: datetime) -> RuleResult:
        if submission.fair_share_weight is None:
            return RuleResult(
                rule_name=self.name, passed=True, detail="no fair-share weight specified"
            )
        if submission.fair_share_weight <= 0:
            return RuleResult(
                rule_name=self.name,
                passed=False,
                detail=f"fair_share_weight must be positive, got {submission.fair_share_weight}",
            )
        return RuleResult(rule_name=self.name, passed=True, detail="fair-share weight is valid")


class ResourceRequirementsRule:
    name = "resource_requirements"

    def evaluate(self, submission: TaskSubmission, *, now: datetime) -> RuleResult:
        requirements = submission.resource_requirements
        if requirements is None:
            return RuleResult(
                rule_name=self.name, passed=True, detail="no resource requirements specified"
            )

        problems = []
        if requirements.min_cpu_cores is not None and requirements.min_cpu_cores <= 0:
            problems.append("min_cpu_cores must be positive")
        if requirements.min_memory_mb is not None and requirements.min_memory_mb <= 0:
            problems.append("min_memory_mb must be positive")
        for field_name in _REQUIREMENT_STRING_FIELDS:
            value = getattr(requirements, field_name)
            if value is not None and not value:
                problems.append(f"{field_name} must not be an empty string")

        if problems:
            return RuleResult(rule_name=self.name, passed=False, detail="; ".join(problems))
        return RuleResult(
            rule_name=self.name, passed=True, detail="resource requirements are valid"
        )
