"""Operational endpoints: the Vercel Cron maintenance sweep and Prometheus
metrics. Both require their own shared secret and are hidden from OpenAPI."""

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from nexus.api.security import require_cron_secret, require_metrics_token, settings_from_request
from nexus.infrastructure.config import Settings
from nexus.infrastructure.execution.maintenance import run_maintenance_pass

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


@router.get("/metrics", dependencies=[Depends(require_metrics_token)])
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
