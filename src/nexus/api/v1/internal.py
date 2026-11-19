"""Operational endpoints: the Vercel Cron maintenance sweep and Prometheus
metrics. Both require their own shared secret and are hidden from OpenAPI."""

from typing import Annotated, Any

from aio_pika.abc import AbstractRobustConnection
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.security import require_cron_secret, require_metrics_token, settings_from_request
from nexus.api.v1.state_metrics import render_state_metrics
from nexus.infrastructure.config import Settings
from nexus.infrastructure.db.session import get_db_session
from nexus.infrastructure.execution.maintenance import run_maintenance_pass
from nexus.infrastructure.messaging.dead_letter import prune_resolved_dead_letters
from nexus.infrastructure.messaging.rabbitmq import get_rabbitmq_connection

router = APIRouter(prefix="/internal", tags=["internal"], include_in_schema=False)


@router.get("/maintenance", dependencies=[Depends(require_cron_secret)])
async def maintenance(
    request: Request, settings: Annotated[Settings, Depends(settings_from_request)]
) -> dict[str, Any]:
    """Vercel Cron issues GET with `Authorization: Bearer $CRON_SECRET`."""
    container = request.app.state.container
    report = await run_maintenance_pass(
        container.db_sessionmaker,
        container.publisher,
        settings,
        replica_id=container.replica_id,
        trigger="cron",
    )
    return {
        "leader": report.leader,
        "fencing_token": report.fencing_token,
        "expired_leases": report.reconcile.expired_leases,
        "redispatched": report.reconcile.redispatched,
        "promoted": report.reconcile.promoted,
        "cancelled_blocked": report.reconcile.cancelled_blocked,
        "relayed": report.relayed,
        "jobs_recovered": report.jobs.total,
        "dead_workers": report.dead_workers,
        "fenced_out": report.fenced_out,
    }


@router.post("/dead-letter/prune", dependencies=[Depends(require_cron_secret)])
async def prune_dead_letters(
    request: Request,
    settings: Annotated[Settings, Depends(settings_from_request)],
    rabbit: Annotated[AbstractRobustConnection | None, Depends(get_rabbitmq_connection)],
) -> dict[str, Any]:
    """Operator / cron: drop nexus.dead-letter messages whose job is no longer
    dead-lettered. Also done by the leader scheduler every pass; this route
    covers deployments without the daemon (e.g. Vercel preview)."""
    if rabbit is None:
        raise HTTPException(
            status_code=503,
            detail={"code": "broker_unavailable", "message": "RabbitMQ is not configured"},
        )
    report = await prune_resolved_dead_letters(
        rabbit, request.app.state.container.db_sessionmaker, limit=settings.dead_letter_prune_batch
    )
    return {"inspected": report.inspected, "pruned": report.pruned, "kept": report.kept}


@router.get("/metrics", dependencies=[Depends(require_metrics_token)])
async def metrics(
    settings: Annotated[Settings, Depends(settings_from_request)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    rabbit: Annotated[AbstractRobustConnection | None, Depends(get_rabbitmq_connection)],
) -> Response:
    """This process's counters, followed by fleet-wide figures read from the
    database and the broker (see `state_metrics`)."""
    body = generate_latest() + await render_state_metrics(session, rabbit, settings)
    return Response(content=body, media_type=CONTENT_TYPE_LATEST)
