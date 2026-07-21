from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from nexus.domain.admission_rules import (
    DeadlineValidityRule,
    FairShareMetadataRule,
    RequiredFieldsRule,
    ResourceRequirementsRule,
    RetryPolicyConsistencyRule,
)
from nexus.domain.task_submission import RetryPolicy, TaskResourceRequirements, TaskSubmission

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def _submission(**overrides) -> TaskSubmission:
    defaults = {
        "task_id": uuid4(),
        "job_id": uuid4(),
        "tenant_id": "acme",
        "retry_policy": RetryPolicy(
            max_attempts=3, initial_backoff_seconds=1.0, max_backoff_seconds=60.0
        ),
    }
    defaults.update(overrides)
    return TaskSubmission(**defaults)


# ---- RequiredFieldsRule ----------------------------------------------------


def test_required_fields_passes_when_present():
    result = RequiredFieldsRule().evaluate(_submission(), now=NOW)
    assert result.passed is True


@pytest.mark.parametrize("tenant_id", [None, ""])
def test_required_fields_fails_when_tenant_id_missing(tenant_id):
    result = RequiredFieldsRule().evaluate(_submission(tenant_id=tenant_id), now=NOW)
    assert result.passed is False
    assert "tenant_id" in result.detail


def test_required_fields_fails_when_retry_policy_missing():
    result = RequiredFieldsRule().evaluate(_submission(retry_policy=None), now=NOW)
    assert result.passed is False
    assert "retry_policy" in result.detail


def test_required_fields_reports_both_missing_fields_together():
    result = RequiredFieldsRule().evaluate(_submission(tenant_id=None, retry_policy=None), now=NOW)
    assert result.passed is False
    assert "tenant_id" in result.detail
    assert "retry_policy" in result.detail


def test_required_fields_does_not_judge_retry_policy_validity():
    """Presence-only: an internally-garbage retry policy still counts as
    'present' for this rule -- validity is RetryPolicyConsistencyRule's job."""
    garbage_policy = RetryPolicy(max_attempts=-1, initial_backoff_seconds=-1)
    result = RequiredFieldsRule().evaluate(_submission(retry_policy=garbage_policy), now=NOW)
    assert result.passed is True


# ---- DeadlineValidityRule ---------------------------------------------------


def test_deadline_validity_passes_when_deadline_is_none():
    result = DeadlineValidityRule().evaluate(_submission(deadline=None), now=NOW)
    assert result.passed is True


def test_deadline_validity_passes_when_deadline_equals_now():
    result = DeadlineValidityRule().evaluate(_submission(deadline=NOW), now=NOW)
    assert result.passed is True


def test_deadline_validity_fails_when_deadline_is_in_the_past():
    result = DeadlineValidityRule().evaluate(
        _submission(deadline=NOW - timedelta(seconds=1)), now=NOW
    )
    assert result.passed is False


def test_deadline_validity_passes_when_not_before_equals_deadline():
    deadline = NOW + timedelta(hours=1)
    result = DeadlineValidityRule().evaluate(
        _submission(deadline=deadline, not_before=deadline), now=NOW
    )
    assert result.passed is True


def test_deadline_validity_fails_when_not_before_is_after_deadline():
    result = DeadlineValidityRule().evaluate(
        _submission(
            deadline=NOW + timedelta(hours=1),
            not_before=NOW + timedelta(hours=2),
        ),
        now=NOW,
    )
    assert result.passed is False
    assert "not_before" in result.detail


def test_deadline_validity_passes_for_well_ordered_future_deadline():
    result = DeadlineValidityRule().evaluate(
        _submission(
            deadline=NOW + timedelta(hours=2),
            not_before=NOW + timedelta(hours=1),
        ),
        now=NOW,
    )
    assert result.passed is True


# ---- RetryPolicyConsistencyRule --------------------------------------------


def test_retry_policy_passes_when_none():
    result = RetryPolicyConsistencyRule().evaluate(_submission(retry_policy=None), now=NOW)
    assert result.passed is True


@pytest.mark.parametrize("max_attempts", [None, 0, -1])
def test_retry_policy_fails_for_invalid_max_attempts(max_attempts):
    policy = RetryPolicy(
        max_attempts=max_attempts, initial_backoff_seconds=1.0, max_backoff_seconds=60.0
    )
    result = RetryPolicyConsistencyRule().evaluate(_submission(retry_policy=policy), now=NOW)
    assert result.passed is False
    assert "max_attempts" in result.detail


def test_retry_policy_passes_for_max_attempts_boundary_of_one():
    policy = RetryPolicy(max_attempts=1, initial_backoff_seconds=0.0, max_backoff_seconds=0.0)
    result = RetryPolicyConsistencyRule().evaluate(_submission(retry_policy=policy), now=NOW)
    assert result.passed is True


@pytest.mark.parametrize("initial_backoff_seconds", [None, -1.0])
def test_retry_policy_fails_for_invalid_initial_backoff(initial_backoff_seconds):
    policy = RetryPolicy(
        max_attempts=3,
        initial_backoff_seconds=initial_backoff_seconds,
        max_backoff_seconds=60.0,
    )
    result = RetryPolicyConsistencyRule().evaluate(_submission(retry_policy=policy), now=NOW)
    assert result.passed is False


def test_retry_policy_fails_when_initial_backoff_exceeds_max_backoff():
    policy = RetryPolicy(max_attempts=3, initial_backoff_seconds=10.0, max_backoff_seconds=5.0)
    result = RetryPolicyConsistencyRule().evaluate(_submission(retry_policy=policy), now=NOW)
    assert result.passed is False
    assert "must not exceed" in result.detail


def test_retry_policy_passes_when_initial_backoff_equals_max_backoff():
    policy = RetryPolicy(max_attempts=3, initial_backoff_seconds=5.0, max_backoff_seconds=5.0)
    result = RetryPolicyConsistencyRule().evaluate(_submission(retry_policy=policy), now=NOW)
    assert result.passed is True


# ---- FairShareMetadataRule ---------------------------------------------------


def test_fair_share_passes_when_weight_is_none():
    result = FairShareMetadataRule().evaluate(_submission(fair_share_weight=None), now=NOW)
    assert result.passed is True


@pytest.mark.parametrize("weight", [0, -1.0])
def test_fair_share_fails_for_non_positive_weight(weight):
    result = FairShareMetadataRule().evaluate(_submission(fair_share_weight=weight), now=NOW)
    assert result.passed is False


def test_fair_share_passes_for_small_positive_weight():
    result = FairShareMetadataRule().evaluate(_submission(fair_share_weight=0.01), now=NOW)
    assert result.passed is True


# ---- ResourceRequirementsRule ------------------------------------------------


def test_resource_requirements_passes_when_none():
    result = ResourceRequirementsRule().evaluate(_submission(resource_requirements=None), now=NOW)
    assert result.passed is True


@pytest.mark.parametrize("min_cpu_cores", [0, -1.0])
def test_resource_requirements_fails_for_invalid_cpu(min_cpu_cores):
    requirements = TaskResourceRequirements(min_cpu_cores=min_cpu_cores)
    result = ResourceRequirementsRule().evaluate(
        _submission(resource_requirements=requirements), now=NOW
    )
    assert result.passed is False
    assert "min_cpu_cores" in result.detail


@pytest.mark.parametrize("min_memory_mb", [0, -1])
def test_resource_requirements_fails_for_invalid_memory(min_memory_mb):
    requirements = TaskResourceRequirements(min_memory_mb=min_memory_mb)
    result = ResourceRequirementsRule().evaluate(
        _submission(resource_requirements=requirements), now=NOW
    )
    assert result.passed is False
    assert "min_memory_mb" in result.detail


@pytest.mark.parametrize("field_name", ["architecture", "region", "runtime"])
def test_resource_requirements_fails_for_empty_string_fields(field_name):
    requirements = TaskResourceRequirements(**{field_name: ""})
    result = ResourceRequirementsRule().evaluate(
        _submission(resource_requirements=requirements), now=NOW
    )
    assert result.passed is False
    assert field_name in result.detail


def test_resource_requirements_reports_multiple_problems_together():
    requirements = TaskResourceRequirements(min_cpu_cores=0, min_memory_mb=0)
    result = ResourceRequirementsRule().evaluate(
        _submission(resource_requirements=requirements), now=NOW
    )
    assert result.passed is False
    assert "min_cpu_cores" in result.detail
    assert "min_memory_mb" in result.detail


def test_resource_requirements_passes_for_fully_valid_requirements():
    requirements = TaskResourceRequirements(
        min_cpu_cores=2.0,
        min_memory_mb=4096,
        architecture="x86_64",
        region="us-east",
        runtime="python3.12",
        custom={"gpu_family": "ampere"},
    )
    result = ResourceRequirementsRule().evaluate(
        _submission(resource_requirements=requirements), now=NOW
    )
    assert result.passed is True
