from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from nexus.application.interfaces.task_store import StoredTransition, TaskRecord
from nexus.domain.admission import AdmissionDecision
from nexus.domain.task_state import TaskState

# Upper bounds only. Semantic validity (max_attempts >= 1, backoff ordering,
# deadline after not_before, ...) is the admission controller's job, so that
# a bad-but-bounded submission gets a structured, rule-named rejection.
_MAX_ATTEMPTS_CAP = 20
_MAX_BACKOFF_CAP_SECONDS = 24 * 3600


class RetryPolicyIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_attempts: int | None = Field(default=None, le=_MAX_ATTEMPTS_CAP)
    initial_backoff_seconds: float | None = Field(default=None, le=_MAX_BACKOFF_CAP_SECONDS)
    max_backoff_seconds: float | None = Field(default=None, le=_MAX_BACKOFF_CAP_SECONDS)


class ResourceRequirementsIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min_cpu_cores: float | None = None
    min_memory_mb: int | None = None
    architecture: str | None = Field(default=None, max_length=64)
    region: str | None = Field(default=None, max_length=64)
    runtime: str | None = Field(default=None, max_length=64)
    custom: dict[str, Any] = Field(default_factory=dict)


class TaskCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_type: str = Field(min_length=1, max_length=100)
    payload: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int | None = Field(default=None, ge=1)
    retry_policy: RetryPolicyIn | None = None
    job_id: UUID | None = None
    depends_on: list[UUID] = Field(default_factory=list, max_length=100)
    deadline: AwareDatetime | None = None
    not_before: AwareDatetime | None = None
    estimated_duration_seconds: float | None = Field(default=None, ge=0)
    fair_share_weight: float | None = Field(default=None, le=1000)
    resource_requirements: ResourceRequirementsIn | None = None


class TaskOut(BaseModel):
    task_id: UUID
    tenant_id: str
    job_id: UUID
    task_type: str
    state: TaskState
    attempt: int
    max_attempts: int
    timeout_seconds: int
    payload: dict[str, Any]
    result: Any = None
    last_error: str | None = None
    admission_reason: str | None = None
    depends_on: list[UUID]
    deadline: datetime | None = None
    not_before: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    lease_expires_at: datetime | None = None

    @classmethod
    def from_record(cls, record: TaskRecord) -> "TaskOut":
        return cls(
            task_id=record.task_id,
            tenant_id=record.tenant_id,
            job_id=record.job_id,
            task_type=record.task_type,
            state=record.state,
            attempt=record.attempt,
            max_attempts=record.max_attempts,
            timeout_seconds=record.timeout_seconds,
            payload=record.payload,
            result=record.result,
            last_error=record.last_error,
            admission_reason=record.admission_reason,
            depends_on=sorted(record.depends_on, key=str),
            deadline=record.deadline,
            not_before=record.not_before,
            created_at=record.created_at,
            updated_at=record.updated_at,
            started_at=record.started_at,
            finished_at=record.finished_at,
            lease_expires_at=record.lease_expires_at,
        )


class RuleResultOut(BaseModel):
    rule_name: str
    passed: bool
    detail: str


class AdmissionOut(BaseModel):
    outcome: str
    reason: str
    validation_results: list[RuleResultOut]

    @classmethod
    def from_decision(cls, decision: AdmissionDecision) -> "AdmissionOut":
        return cls(
            outcome=decision.outcome.value,
            reason=decision.reason,
            validation_results=[
                RuleResultOut(rule_name=r.rule_name, passed=r.passed, detail=r.detail)
                for r in decision.validation_results
            ],
        )


class SubmitTaskOut(BaseModel):
    task: TaskOut
    created: bool
    admission: AdmissionOut | None = None


class TaskListOut(BaseModel):
    items: list[TaskOut]
    next_created_before: datetime | None = None


class TransitionOut(BaseModel):
    attempt: int
    from_state: TaskState
    to_state: TaskState
    occurred_at: datetime
    reason: str | None

    @classmethod
    def from_stored(cls, stored: StoredTransition) -> "TransitionOut":
        return cls(
            attempt=stored.attempt,
            from_state=stored.from_state,
            to_state=stored.to_state,
            occurred_at=stored.occurred_at,
            reason=stored.reason,
        )


class CancelIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = Field(default="cancelled by client", max_length=500)
