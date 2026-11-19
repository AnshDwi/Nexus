"""Read models for the dashboard: workers, queue, execution history, the
overview summary and system information."""

import asyncio
import contextlib
from datetime import datetime, timedelta
from typing import Annotated, Any
from uuid import UUID

from aio_pika.abc import AbstractRobustConnection
from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel
from redis.asyncio import Redis
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.rate_limit import rate_limit
from nexus.api.security import Principal, settings_from_request
from nexus.api.v1.jobs import ExecutionOut, JobOut
from nexus.domain.job import JobState
from nexus.infrastructure.cache.redis import get_redis
from nexus.infrastructure.config import Settings
from nexus.infrastructure.coordination.worker_registry import PostgresWorkerRegistry
from nexus.infrastructure.db.models.outbox import OutboxRecordModel
from nexus.infrastructure.db.repositories.job_repository import PostgresJobStore
from nexus.infrastructure.db.session import get_db_session
from nexus.infrastructure.messaging.rabbitmq import get_rabbitmq_connection
from nexus.infrastructure.messaging.rabbitmq_topology import DEAD_LETTER_QUEUE, JOBS_QUEUE

router = APIRouter(tags=["platform"])

Reader = Annotated[Principal, Depends(rate_limit("read"))]
Session = Annotated[AsyncSession, Depends(get_db_session)]
AppSettings = Annotated[Settings, Depends(settings_from_request)]
Rabbit = Annotated[AbstractRobustConnection | None, Depends(get_rabbitmq_connection)]
_PROBE_TIMEOUT = 2.0
APP_VERSION = "0.2.0"


# --- workers -----------------------------------------------------------------------


class WorkerOut(BaseModel):
    worker_id: UUID
    status: str  # healthy | unhealthy | draining
    healthy: bool
    last_heartbeat_at: datetime
    heartbeat_age_seconds: float
    registered_at: datetime
    capacity_slots: int
    running_jobs: int
    utilization: float
    capabilities: dict[str, Any]


async def _workers(session: AsyncSession, settings: Settings) -> list[WorkerOut]:
    registry = PostgresWorkerRegistry(session)
    now = await registry.database_now()
    running = await PostgresJobStore(session).running_by_worker()
    ttl = settings.worker_heartbeat_ttl_seconds
    out = []
    for w in await registry.list_workers():
        age = max((now - w.last_heartbeat_at).total_seconds(), 0.0)
        healthy = age <= ttl
        status = "draining" if w.draining else ("healthy" if healthy else "unhealthy")
        busy = running.get(w.worker_id, 0)
        out.append(
            WorkerOut(
                worker_id=w.worker_id,
                status=status,
                healthy=healthy and not w.draining,
                last_heartbeat_at=w.last_heartbeat_at,
                heartbeat_age_seconds=round(age, 1),
                registered_at=w.registered_at,
                capacity_slots=w.capacity_slots,
                running_jobs=busy,
                utilization=round(busy / w.capacity_slots, 2) if w.capacity_slots else 0.0,
                capabilities={
                    "cpu_cores": w.capabilities.cpu_cores,
                    "memory_mb": w.capabilities.memory_mb,
                    "architecture": w.capabilities.architecture,
                    "region": w.capabilities.region,
                    "runtime": w.capabilities.runtime,
                },
            )
        )
    return out


@router.get("/workers", response_model=list[WorkerOut])
async def list_workers(_: Reader, session: Session, settings: AppSettings) -> list[WorkerOut]:
    return await _workers(session, settings)


# --- queue -------------------------------------------------------------------------


class BrokerDepth(BaseModel):
    available: bool
    ready_messages: int | None = None
    consumers: int | None = None
    dead_letter_messages: int | None = None


async def _broker_depth(connection: AbstractRobustConnection | None) -> BrokerDepth:
    if connection is None:
        return BrokerDepth(available=False)

    async def probe() -> BrokerDepth:
        channel = await connection.channel()
        try:
            jobs = await channel.declare_queue(JOBS_QUEUE, passive=True)
            dlq = await channel.declare_queue(DEAD_LETTER_QUEUE, passive=True)
            return BrokerDepth(
                available=True,
                ready_messages=jobs.declaration_result.message_count,
                consumers=jobs.declaration_result.consumer_count,
                dead_letter_messages=dlq.declaration_result.message_count,
            )
        finally:
            with contextlib.suppress(Exception):
                await channel.close()

    try:
        return await asyncio.wait_for(probe(), _PROBE_TIMEOUT)
    except Exception:
        return BrokerDepth(available=False)


class QueueItemOut(BaseModel):
    job: JobOut
    dispatch_status: str  # awaiting_publish | scheduled_retry | confirmed
    dispatch_requested_at: datetime | None


class QueueOut(BaseModel):
    pending: int
    queued: int
    retrying: int
    depth: int
    dead_lettered_jobs: int
    broker: BrokerDepth
    unpublished_events: int
    items: list[QueueItemOut]


@router.get("/queue", response_model=QueueOut)
async def queue(
    principal: Reader,
    session: Session,
    rabbit: Rabbit,
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
) -> QueueOut:
    store = PostgresJobStore(session)
    counts = await store.counts_by_state(principal.tenant_id)
    now = await store.now()
    items = []
    for row in await store.queue_snapshot(principal.tenant_id, limit):
        job = row["job"]
        if job.state is JobState.QUEUED:
            dispatch = "confirmed"
        elif job.state is JobState.RETRYING and job.next_attempt_at and job.next_attempt_at > now:
            dispatch = "scheduled_retry"
        else:
            dispatch = "awaiting_publish"
        items.append(
            QueueItemOut(
                job=JobOut.of(job),
                dispatch_status=dispatch,
                dispatch_requested_at=row["dispatch_requested_at"],
            )
        )
    unpublished = (
        await session.execute(
            select(func.count()).where(
                OutboxRecordModel.dispatched_at.is_(None),
                OutboxRecordModel.failed_permanently_at.is_(None),
                OutboxRecordModel.aggregate_type == "job",
            )
        )
    ).scalar_one()
    pending, queued, retrying = (
        counts[JobState.PENDING],
        counts[JobState.QUEUED],
        counts[JobState.RETRYING],
    )
    return QueueOut(
        pending=pending,
        queued=queued,
        retrying=retrying,
        depth=pending + queued + retrying,
        dead_lettered_jobs=await store.count_dead_lettered(principal.tenant_id),
        broker=await _broker_depth(rabbit),
        unpublished_events=unpublished,
        items=items,
    )


# --- execution history ----------------------------------------------------------------


class ExecutionPageOut(BaseModel):
    items: list[ExecutionOut]
    total: int
    page: int
    page_size: int
    pages: int


@router.get("/executions", response_model=ExecutionPageOut)
async def executions(
    principal: Reader,
    session: Session,
    status: Annotated[str | None, Query(pattern=r"^[a-z_]+$")] = None,
    job_id: UUID | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 25,
) -> ExecutionPageOut:
    result = await PostgresJobStore(session).list_executions(
        principal.tenant_id, job_id=job_id, status=status, page=page, page_size=page_size
    )
    return ExecutionPageOut(
        items=[ExecutionOut.of(e) for e in result.items],
        total=result.total,
        page=page,
        page_size=page_size,
        pages=max((result.total + page_size - 1) // page_size, 1),
    )


# --- overview ------------------------------------------------------------------------


class ActivityOut(BaseModel):
    job_id: UUID
    job_name: str
    from_state: JobState
    to_state: JobState
    occurred_at: datetime
    attempt: int
    reason: str | None


class TrendPointOut(BaseModel):
    bucket_start: datetime
    completed: int
    failed: int


class WorkerHealthOut(BaseModel):
    total: int
    healthy: int
    unhealthy: int
    draining: int
    capacity: int
    busy: int


class SummaryOut(BaseModel):
    total_jobs: int
    by_state: dict[str, int]
    running: int
    completed: int
    failed: int
    dead_lettered: int
    queue_depth: int
    broker: BrokerDepth
    workers: WorkerHealthOut
    recent_activity: list[ActivityOut]
    trend: list[TrendPointOut]
    generated_at: datetime


@router.get("/metrics/summary", response_model=SummaryOut)
async def summary(
    principal: Reader,
    session: Session,
    settings: AppSettings,
    rabbit: Rabbit,
    hours: Annotated[int, Query(ge=1, le=168)] = 24,
) -> SummaryOut:
    store = PostgresJobStore(session)
    now = await store.now()
    counts = await store.counts_by_state(principal.tenant_id)
    workers = await _workers(session, settings)
    since = (now - timedelta(hours=hours)).replace(minute=0, second=0, microsecond=0)
    raw = {
        b.bucket_start: b
        for b in await store.execution_trend(principal.tenant_id, since=since, bucket_minutes=60)
    }
    trend = []
    bucket = since
    while bucket <= now:
        hit = raw.get(bucket)
        trend.append(
            TrendPointOut(
                bucket_start=bucket,
                completed=hit.completed if hit else 0,
                failed=hit.failed if hit else 0,
            )
        )
        bucket += timedelta(hours=1)
    return SummaryOut(
        total_jobs=sum(counts.values()),
        by_state={s.value: c for s, c in counts.items()},
        running=counts[JobState.RUNNING],
        completed=counts[JobState.COMPLETED],
        failed=counts[JobState.FAILED],
        dead_lettered=await store.count_dead_lettered(principal.tenant_id),
        queue_depth=counts[JobState.PENDING] + counts[JobState.QUEUED] + counts[JobState.RETRYING],
        broker=await _broker_depth(rabbit),
        workers=WorkerHealthOut(
            total=len(workers),
            healthy=sum(1 for w in workers if w.status == "healthy"),
            unhealthy=sum(1 for w in workers if w.status == "unhealthy"),
            draining=sum(1 for w in workers if w.status == "draining"),
            capacity=sum(w.capacity_slots for w in workers if w.status == "healthy"),
            busy=sum(w.running_jobs for w in workers),
        ),
        recent_activity=[
            ActivityOut(
                job_id=a.job_id,
                job_name=a.job_name,
                from_state=a.from_state,
                to_state=a.to_state,
                occurred_at=a.occurred_at,
                attempt=a.attempt,
                reason=a.reason,
            )
            for a in await store.recent_activity(principal.tenant_id, 15)
        ],
        trend=trend,
        generated_at=now,
    )


# --- system --------------------------------------------------------------------------


class SystemOut(BaseModel):
    version: str
    environment: str
    tenant_id: str
    event_bus: str
    checks: dict[str, str]
    job_types: list[str]
    config: dict[str, Any]


@router.get("/system", response_model=SystemOut)
async def system(
    request: Request,
    principal: Reader,
    session: Session,
    settings: AppSettings,
    rabbit: Rabbit,
    redis: Annotated[Redis | None, Depends(get_redis)],
) -> SystemOut:
    checks: dict[str, str] = {}
    try:
        await asyncio.wait_for(session.execute(text("SELECT 1")), _PROBE_TIMEOUT)
        checks["postgres"] = "ok"
    except Exception:
        checks["postgres"] = "error"
    if redis is not None:
        try:
            await asyncio.wait_for(redis.ping(), _PROBE_TIMEOUT)
            checks["redis"] = "ok"
        except Exception:
            checks["redis"] = "error"
    else:
        checks["redis"] = "not_configured"
    broker = await _broker_depth(rabbit)
    checks["rabbitmq"] = (
        "ok" if broker.available else ("not_configured" if rabbit is None else "error")
    )
    registry = request.app.state.container.task_handlers
    return SystemOut(
        version=APP_VERSION,
        environment=settings.environment,
        tenant_id=principal.tenant_id,
        event_bus=settings.event_bus.value,
        checks=checks,
        job_types=sorted(registry.task_types()),
        config={
            "job_default_timeout_seconds": settings.job_default_timeout_seconds,
            "job_max_timeout_seconds": settings.job_max_timeout_seconds,
            "job_default_max_attempts": settings.job_default_max_attempts,
            "worker_heartbeat_ttl_seconds": settings.worker_heartbeat_ttl_seconds,
            "rate_limit_reads_per_minute": settings.rate_limit_reads_per_minute,
            "rate_limit_writes_per_minute": settings.rate_limit_writes_per_minute,
            "cors_allowed_origins": settings.cors_origin_list,
            "db_pool_mode": settings.db_pool_mode.value,
        },
    )
