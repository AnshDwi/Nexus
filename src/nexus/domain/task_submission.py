from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """A proposed retry policy from an incoming submission.

    Permissive by design: unlike WorkerCapabilities (Phase 2), this
    describes untrusted boundary input, not a process's self-reported real
    properties. Invalidity here is an expected business outcome the
    admission controller must be able to report structurally (see
    RetryPolicyConsistencyRule), not a programming error to raise on --
    validating at construction would make the invalid case impossible to
    even represent, let alone explain.
    """

    max_attempts: int | None = None
    initial_backoff_seconds: float | None = None
    max_backoff_seconds: float | None = None


@dataclass(frozen=True, slots=True)
class TaskResourceRequirements:
    """What a task claims to need, as declared by the submitter.

    Structurally mirrors WorkerCapabilities (cpu/memory/architecture/region/
    runtime/custom) since a future phase will match one against the other --
    but every field is optional (no constraint = any worker qualifies) and
    nothing is validated at construction, for the same boundary-input reason
    as RetryPolicy. See ResourceRequirementsRule.
    """

    min_cpu_cores: float | None = None
    min_memory_mb: int | None = None
    architecture: str | None = None
    region: str | None = None
    runtime: str | None = None
    custom: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TaskSubmission:
    """The admission controller's input: an untrusted request at a system
    boundary, not a validated domain fact. Every field beyond bare identity
    is optional/permissive on purpose -- that is what lets the controller
    evaluate and explain *why* something is invalid, rather than the caller
    never being able to construct the invalid case in the first place.
    """

    task_id: UUID
    job_id: UUID
    tenant_id: str | None = None
    deadline: datetime | None = None
    not_before: datetime | None = None
    estimated_duration: timedelta | None = None
    retry_policy: RetryPolicy | None = None
    fair_share_weight: float | None = None
    resource_requirements: TaskResourceRequirements | None = None
