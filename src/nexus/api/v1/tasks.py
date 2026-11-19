import json
import time
from datetime import datetime, timedelta
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.security import Principal, authenticate, settings_from_request
from nexus.api.v1.schemas import (
    AdmissionOut,
    CancelIn,
    SubmitTaskOut,
    TaskCreate,
    TaskListOut,
    TaskOut,
    TransitionOut,
)
from nexus.application.interfaces.task_handler import TaskHandlerRegistry
from nexus.application.use_cases.admission_controller import AdmissionController
from nexus.application.use_cases.submit_task import SubmitTaskCommand, submit_task
from nexus.application.use_cases.task_lifecycle import cancel_task
from nexus.domain.admission import AdmissionOutcome
from nexus.domain.task_state import InvalidTaskTransitionError, TaskState
from nexus.domain.task_submission import RetryPolicy, TaskResourceRequirements
from nexus.infrastructure.config import Settings
from nexus.infrastructure.db.repositories.outbox_repository import PostgresOutboxWriter
from nexus.infrastructure.db.repositories.task_repository import PostgresTaskStore
from nexus.infrastructure.db.session import get_db_session
from nexus.infrastructure.messaging.relay import relay_pending_events
from nexus.infrastructure.observability.admission_metrics import record_admission_decision
from nexus.infrastructure.observability.metrics import outbox_relayed_total, task_submissions_total

router = APIRouter(prefix="/tasks", tags=["tasks"])

_admission = AdmissionController()


def _handlers(request: Request) -> TaskHandlerRegistry:
    registry: TaskHandlerRegistry = request.app.state.container.task_handlers
    return registry


async def _eager_relay(request: Request, settings: Settings) -> None:
    """Publish the rows this request just committed so the task is dispatched
    within this invocation. Best effort: anything left over is relayed by the
    next sweep, and the queue's idempotency key absorbs double publishes."""
    container = request.app.state.container
    relayed = await relay_pending_events(
        container.db_sessionmaker,
        container.publisher,
        settings,
        max_events=settings.outbox_eager_relay_max_events,
    )
    outbox_relayed_total.labels(trigger="eager").inc(relayed)


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=SubmitTaskOut,
    responses={200: {"model": SubmitTaskOut}, 422: {"model": SubmitTaskOut}},
)
async def create_task(
    body: TaskCreate,
    request: Request,
    principal: Annotated[Principal, Depends(authenticate)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    settings: Annotated[Settings, Depends(settings_from_request)],
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key", max_length=200)] = None,
) -> Response:
    if len(json.dumps(body.payload)) > settings.task_max_payload_bytes:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "payload too large")
    timeout = body.timeout_seconds or settings.task_default_timeout_seconds
    if timeout > settings.task_max_timeout_seconds:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"timeout_seconds must be <= {settings.task_max_timeout_seconds}",
        )

    command = SubmitTaskCommand(
        tenant_id=principal.tenant_id,
        task_type=body.task_type,
        payload=body.payload,
        timeout_seconds=timeout,
        retry_policy=RetryPolicy(**body.retry_policy.model_dump()) if body.retry_policy else None,
        job_id=body.job_id,
        deadline=body.deadline,
        not_before=body.not_before,
        estimated_duration=(
            timedelta(seconds=body.estimated_duration_seconds)
            if body.estimated_duration_seconds is not None
            else None
        ),
        fair_share_weight=body.fair_share_weight,
        depends_on=tuple(body.depends_on),
        resource_requirements=(
            TaskResourceRequirements(**body.resource_requirements.model_dump())
            if body.resource_requirements
            else None
        ),
        idempotency_key=idempotency_key,
    )

    started = time.perf_counter()
    result = await submit_task(
        PostgresTaskStore(session),
        PostgresOutboxWriter(session),
        _handlers(request),
        _admission,
        command,
    )
    await session.commit()

    if result.decision is not None:
        record_admission_decision(
            result.decision, evaluation_duration_seconds=time.perf_counter() - started
        )
    if not result.created:
        task_submissions_total.labels(outcome="duplicate").inc()
        out = SubmitTaskOut(task=TaskOut.from_record(result.task), created=False)
        return JSONResponse(status_code=200, content=out.model_dump(mode="json"))

    await _eager_relay(request, settings)
    rejected = result.decision is not None and result.decision.outcome is AdmissionOutcome.REJECTED
    task_submissions_total.labels(outcome="rejected" if rejected else "created").inc()
    out = SubmitTaskOut(
        task=TaskOut.from_record(result.task),
        created=True,
        admission=AdmissionOut.from_decision(result.decision) if result.decision else None,
    )
    return JSONResponse(
        status_code=422 if rejected else 201,
        content=out.model_dump(mode="json"),
        headers={"Location": f"{request.url.path}/{result.task.task_id}"},
    )


@router.get("", response_model=TaskListOut)
async def list_tasks(
    principal: Annotated[Principal, Depends(authenticate)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    state: TaskState | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    created_before: datetime | None = None,
) -> TaskListOut:
    records = await PostgresTaskStore(session).list_for_tenant(
        principal.tenant_id, state=state, limit=limit, created_before=created_before
    )
    next_cursor = records[-1].created_at if len(records) == limit else None
    return TaskListOut(
        items=[TaskOut.from_record(r) for r in records], next_created_before=next_cursor
    )


@router.get("/{task_id}", response_model=TaskOut)
async def get_task(
    task_id: UUID,
    principal: Annotated[Principal, Depends(authenticate)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> TaskOut:
    record = await PostgresTaskStore(session).get(task_id, tenant_id=principal.tenant_id)
    if record is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "task not found")
    return TaskOut.from_record(record)


@router.get("/{task_id}/transitions", response_model=list[TransitionOut])
async def get_task_transitions(
    task_id: UUID,
    principal: Annotated[Principal, Depends(authenticate)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> list[TransitionOut]:
    store = PostgresTaskStore(session)
    if await store.get(task_id, tenant_id=principal.tenant_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "task not found")
    return [TransitionOut.from_stored(t) for t in await store.list_transitions(task_id)]


@router.post("/{task_id}/cancel", response_model=TaskOut)
async def cancel(
    task_id: UUID,
    request: Request,
    principal: Annotated[Principal, Depends(authenticate)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    settings: Annotated[Settings, Depends(settings_from_request)],
    body: CancelIn | None = None,
) -> TaskOut:
    reason = body.reason if body else "cancelled by client"
    try:
        record = await cancel_task(
            PostgresTaskStore(session),
            PostgresOutboxWriter(session),
            task_id=task_id,
            tenant_id=principal.tenant_id,
            reason=reason,
        )
    except InvalidTaskTransitionError as exc:
        await session.rollback()
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    if record is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "task not found")
    await session.commit()
    await _eager_relay(request, settings)
    return TaskOut.from_record(record)
