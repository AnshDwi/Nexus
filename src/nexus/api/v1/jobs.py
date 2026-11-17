from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.rate_limit import rate_limit
from nexus.api.security import Principal, settings_from_request
from nexus.application.interfaces.job_store import JobExecution, JobQuery
from nexus.application.interfaces.task_handler import TaskHandlerRegistry
from nexus.application.use_cases.admission_controller import AdmissionController
from nexus.application.use_cases.jobs import (
    CreateJobCommand,
    JobNotFoundError,
    JobRejectedError,
    cancel_job,
    create_job,
    retry_job,
)
from nexus.domain.job import (
    CANCELLABLE_JOB_STATES,
    InvalidJobTransitionError,
    Job,
    JobNotRetryableError,
    JobPriority,
    JobState,
    JobTransition,
)
from nexus.infrastructure.config import Settings
from nexus.infrastructure.db.repositories.job_repository import PostgresJobStore
from nexus.infrastructure.db.repositories.outbox_repository import PostgresOutboxWriter
from nexus.infrastructure.db.session import get_db_session
from nexus.infrastructure.messaging.relay import relay_pending_events
from nexus.infrastructure.observability.logging import correlation_id_var
from nexus.infrastructure.observability.metrics import job_submissions_total

router = APIRouter(prefix="/jobs", tags=["jobs"])
_admission = AdmissionController()

Reader = Annotated[Principal, Depends(rate_limit("read"))]
Writer = Annotated[Principal, Depends(rate_limit("write"))]
Session = Annotated[AsyncSession, Depends(get_db_session)]
AppSettings = Annotated[Settings, Depends(settings_from_request)]


def _error(code: int, kind: str, message: str, details: Any = None) -> HTTPException:
    detail: dict[str, Any] = {"code": kind, "message": message}
    if details is not None:
        detail["details"] = details
    return HTTPException(status_code=code, detail=detail)


# --- schemas -------------------------------------------------------------------


class JobCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=200)
    job_type: str = Field(min_length=1, max_length=100, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: JobPriority = JobPriority.NORMAL
    max_attempts: int | None = Field(default=None, ge=1, le=20)
    timeout_seconds: int | None = Field(default=None, ge=1)
    initial_backoff_seconds: float = Field(default=2.0, ge=0, le=3600)
    max_backoff_seconds: float = Field(default=300.0, ge=0, le=86400)


class JobOut(BaseModel):
    job_id: UUID
    name: str
    job_type: str
    priority: JobPriority
    state: JobState
    attempt: int
    max_attempts: int
    attempts_remaining: int
    timeout_seconds: int
    payload: dict[str, Any]
    result: Any = None
    last_error: str | None = None
    worker_id: UUID | None = None
    correlation_id: str | None = None
    created_at: datetime
    updated_at: datetime
    queued_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    next_attempt_at: datetime | None = None
    dead_lettered_at: datetime | None = None
    can_cancel: bool
    can_retry: bool

    @classmethod
    def of(cls, job: Job) -> "JobOut":
        return cls(
            job_id=job.job_id,
            name=job.name,
            job_type=job.job_type,
            priority=job.priority,
            state=job.state,
            attempt=job.attempt,
            max_attempts=job.max_attempts,
            attempts_remaining=job.attempts_remaining,
            timeout_seconds=job.timeout_seconds,
            payload=job.payload,
            result=job.result,
            last_error=job.last_error,
            worker_id=job.worker_id,
            correlation_id=job.correlation_id,
            created_at=job.created_at,
            updated_at=job.updated_at,
            queued_at=job.queued_at,
            started_at=job.started_at,
            finished_at=job.finished_at,
            next_attempt_at=job.next_attempt_at,
            dead_lettered_at=job.dead_lettered_at,
            can_cancel=job.state in CANCELLABLE_JOB_STATES,
            can_retry=job.state is JobState.FAILED,
        )


class TransitionOut(BaseModel):
    from_state: JobState
    to_state: JobState
    occurred_at: datetime
    attempt: int
    reason: str | None

    @classmethod
    def of(cls, t: JobTransition) -> "TransitionOut":
        return cls(
            from_state=t.from_state,
            to_state=t.to_state,
            occurred_at=t.occurred_at,
            attempt=t.attempt,
            reason=t.reason,
        )


class ExecutionOut(BaseModel):
    execution_id: int
    job_id: UUID
    job_name: str
    attempt: int
    worker_id: UUID | None
    status: str
    started_at: datetime
    finished_at: datetime | None
    duration_ms: int | None
    error: str | None

    @classmethod
    def of(cls, e: JobExecution) -> "ExecutionOut":
        return cls(
            execution_id=e.execution_id,
            job_id=e.job_id,
            job_name=e.job_name,
            attempt=e.attempt,
            worker_id=e.worker_id,
            status=e.status,
            started_at=e.started_at,
            finished_at=e.finished_at,
            duration_ms=e.duration_ms,
            error=e.error,
        )


class JobDetailOut(BaseModel):
    job: JobOut
    transitions: list[TransitionOut]
    executions: list[ExecutionOut]


class JobPageOut(BaseModel):
    items: list[JobOut]
    total: int
    page: int
    page_size: int
    pages: int


class CancelIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reason: str = Field(default="cancelled by user", min_length=1, max_length=500)


# --- helpers ---------------------------------------------------------------------


def _handlers(request: Request) -> TaskHandlerRegistry:
    registry: TaskHandlerRegistry = request.app.state.container.task_handlers
    return registry


async def _relay(request: Request, settings: Settings) -> None:
    """Publish what this request committed, so the job reaches QUEUED within
    the request when the broker is up. Best effort: the scheduler's relay
    loop delivers anything left over."""
    container = request.app.state.container
    await relay_pending_events(
        container.db_sessionmaker,
        container.publisher,
        settings,
        max_events=settings.outbox_eager_relay_max_events,
    )


async def _reload(session: AsyncSession, job_id: UUID, tenant_id: str) -> Job:
    job = await PostgresJobStore(session).get(job_id, tenant_id=tenant_id)
    if job is None:
        raise _error(404, "not_found", "job not found")
    return job


# --- routes ----------------------------------------------------------------------


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=JobOut,
    responses={200: {"model": JobOut, "description": "Idempotent replay"}},
)
async def create(
    body: JobCreate,
    request: Request,
    principal: Writer,
    session: Session,
    settings: AppSettings,
    idempotency_key: Annotated[
        str | None, Header(alias="Idempotency-Key", min_length=1, max_length=200)
    ] = None,
) -> Any:
    import json

    if len(json.dumps(body.payload)) > settings.task_max_payload_bytes:
        raise _error(413, "payload_too_large", "payload exceeds the size limit")
    timeout = body.timeout_seconds or settings.job_default_timeout_seconds
    if timeout > settings.job_max_timeout_seconds:
        raise _error(
            422, "invalid_timeout", f"timeout_seconds must be <= {settings.job_max_timeout_seconds}"
        )
    try:
        job, created = await create_job(
            PostgresJobStore(session),
            PostgresOutboxWriter(session),
            _handlers(request),
            _admission,
            CreateJobCommand(
                tenant_id=principal.tenant_id,
                name=body.name.strip(),
                job_type=body.job_type,
                payload=body.payload,
                priority=body.priority,
                max_attempts=body.max_attempts or settings.job_default_max_attempts,
                initial_backoff_seconds=body.initial_backoff_seconds,
                max_backoff_seconds=body.max_backoff_seconds,
                timeout_seconds=timeout,
                idempotency_key=idempotency_key,
                correlation_id=correlation_id_var.get(),
            ),
        )
    except JobRejectedError as exc:
        await session.rollback()
        job_submissions_total.labels(outcome="rejected").inc()
        raise _error(
            422,
            "job_rejected",
            str(exc),
            [{"rule": r.rule_name, "detail": r.detail} for r in exc.reasons],
        ) from exc
    await session.commit()
    if not created:
        job_submissions_total.labels(outcome="duplicate").inc()
        return JSONResponse(status_code=200, content=JobOut.of(job).model_dump(mode="json"))
    job_submissions_total.labels(outcome="created").inc()
    await _relay(request, settings)
    session.expire_all()
    return JobOut.of(await _reload(session, job.job_id, principal.tenant_id))


@router.get("", response_model=JobPageOut)
async def list_jobs(
    principal: Reader,
    session: Session,
    status_: Annotated[list[JobState] | None, Query(alias="status")] = None,
    priority: Annotated[list[JobPriority] | None, Query()] = None,
    q: Annotated[str | None, Query(max_length=200)] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 25,
) -> JobPageOut:
    result = await PostgresJobStore(session).search(
        JobQuery(
            tenant_id=principal.tenant_id,
            states=frozenset(status_ or ()),
            priorities=frozenset(priority or ()),
            search=q or None,
            page=page,
            page_size=page_size,
        )
    )
    return JobPageOut(
        items=[JobOut.of(j) for j in result.items],
        total=result.total,
        page=result.page,
        page_size=result.page_size,
        pages=max((result.total + page_size - 1) // page_size, 1),
    )


@router.get("/{job_id}", response_model=JobDetailOut)
async def get_job(job_id: UUID, principal: Reader, session: Session) -> JobDetailOut:
    store = PostgresJobStore(session)
    job = await _reload(session, job_id, principal.tenant_id)
    executions = await store.list_executions(
        principal.tenant_id, job_id=job_id, status=None, page=1, page_size=100
    )
    return JobDetailOut(
        job=JobOut.of(job),
        transitions=[TransitionOut.of(t) for t in await store.list_transitions(job_id)],
        executions=[ExecutionOut.of(e) for e in executions.items],
    )


@router.post("/{job_id}/cancel", response_model=JobOut)
async def cancel(
    job_id: UUID,
    principal: Writer,
    session: Session,
    body: CancelIn | None = None,
) -> JobOut:
    try:
        job = await cancel_job(
            PostgresJobStore(session),
            job_id=job_id,
            tenant_id=principal.tenant_id,
            reason=body.reason if body else "cancelled by user",
        )
    except JobNotFoundError as exc:
        raise _error(404, "not_found", "job not found") from exc
    except InvalidJobTransitionError as exc:
        await session.rollback()
        raise _error(409, "not_cancellable", str(exc)) from exc
    await session.commit()
    return JobOut.of(job)


@router.post("/{job_id}/retry", response_model=JobOut)
async def retry(
    job_id: UUID,
    request: Request,
    principal: Writer,
    session: Session,
    settings: AppSettings,
) -> JobOut:
    try:
        job = await retry_job(
            PostgresJobStore(session),
            PostgresOutboxWriter(session),
            job_id=job_id,
            tenant_id=principal.tenant_id,
        )
    except JobNotFoundError as exc:
        raise _error(404, "not_found", "job not found") from exc
    except (JobNotRetryableError, InvalidJobTransitionError) as exc:
        await session.rollback()
        raise _error(409, "not_retryable", str(exc)) from exc
    await session.commit()
    await _relay(request, settings)
    session.expire_all()
    return JobOut.of(await _reload(session, job.job_id, principal.tenant_id))
