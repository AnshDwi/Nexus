"""Fleet-wide metrics computed from durable state at scrape time.

The `prometheus_client` counters in `observability/metrics.py` live in the
memory of whichever process incremented them: the worker's execution counters
are invisible to the API, and on Vercel every function instance starts from
zero. The figures here are read from PostgreSQL and the broker instead, so any
API instance returns the same, complete answer, and a restart loses nothing.

`nexus_job_executions_total` and the duration sum/count are exposed as
counters: they only grow while execution rows are kept, so `rate()` works on
them. Deleting history (retention) shows up as an ordinary counter reset.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from aio_pika.abc import AbstractRobustConnection
from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, SummaryMetricFamily
from prometheus_client.registry import Collector
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.job import JobState
from nexus.infrastructure.config import Settings

_Family = GaugeMetricFamily | CounterMetricFamily | SummaryMetricFamily


class _Static(Collector):
    def __init__(self, families: Iterable[_Family]) -> None:
        self._families = list(families)

    def collect(self) -> Iterator[_Family]:
        yield from self._families


async def _job_families(session: AsyncSession) -> list[_Family]:
    jobs = GaugeMetricFamily(
        "nexus_jobs", "Jobs currently in each state.", labels=["tenant", "state"]
    )
    rows = (
        await session.execute(
            text("SELECT tenant_id, state, count(*) FROM jobs GROUP BY tenant_id, state")
        )
    ).all()
    seen: dict[str, set[str]] = {}
    for tenant, state, count in rows:
        jobs.add_metric([tenant, state], count)
        seen.setdefault(tenant, set()).add(state)
    for tenant, states in seen.items():  # explicit zeros keep series continuous
        for state in JobState:
            if state.value not in states:
                jobs.add_metric([tenant, state.value], 0)

    dead = GaugeMetricFamily(
        "nexus_jobs_dead_lettered",
        "Jobs currently dead-lettered (final failure, not since retried).",
        labels=["tenant"],
    )
    dead_counts = {
        tenant: count
        for tenant, count in (
            await session.execute(
                text(
                    "SELECT tenant_id, count(*) FROM jobs "
                    "WHERE dead_lettered_at IS NOT NULL GROUP BY tenant_id"
                )
            )
        ).all()
    }
    for tenant in seen:
        dead.add_metric([tenant], dead_counts.get(tenant, 0))

    oldest = GaugeMetricFamily(
        "nexus_jobs_oldest_waiting_seconds",
        "Age of the oldest job waiting to start (PENDING or QUEUED); 0 when none.",
    )
    age = (
        await session.execute(
            text(
                "SELECT COALESCE(EXTRACT(EPOCH FROM"
                " now() - min(COALESCE(queued_at, created_at))), 0)"
                " FROM jobs WHERE state IN ('pending', 'queued')"
            )
        )
    ).scalar_one()
    oldest.add_metric([], float(age))

    executions = CounterMetricFamily(
        "nexus_job_executions",
        "Job attempts recorded, by type and final status (from job_executions).",
        labels=["job_type", "status"],
    )
    duration = SummaryMetricFamily(
        "nexus_job_execution_duration_seconds",
        "Duration of finished job attempts (sum and count; use rate() for averages).",
        labels=["job_type"],
    )
    durations: dict[str, tuple[float, int]] = {}
    for job_type, status, count, total_ms, timed in (
        await session.execute(
            text(
                "SELECT j.job_type, e.status, count(*), "
                "COALESCE(sum(e.duration_ms), 0), count(e.duration_ms) "
                "FROM job_executions e JOIN jobs j USING (job_id) "
                "GROUP BY j.job_type, e.status"
            )
        )
    ).all():
        executions.add_metric([job_type, status], count)
        seconds, finished = durations.get(job_type, (0.0, 0))
        durations[job_type] = (seconds + float(total_ms) / 1000, finished + int(timed))
    for job_type, (seconds, count) in durations.items():
        duration.add_metric([job_type], count_value=count, sum_value=seconds)
    return [jobs, dead, oldest, executions, duration]


async def _outbox_families(session: AsyncSession) -> list[_Family]:
    pending, lag, failed = (
        await session.execute(
            text(
                "SELECT "
                "count(*) FILTER (WHERE dispatched_at IS NULL AND failed_permanently_at IS NULL"
                "  AND available_at <= now()), "
                "COALESCE(EXTRACT(EPOCH FROM now() - min(available_at) FILTER ("
                "  WHERE dispatched_at IS NULL AND failed_permanently_at IS NULL"
                "  AND available_at <= now())), 0), "
                "count(*) FILTER (WHERE failed_permanently_at IS NOT NULL) "
                "FROM outbox_records"
            )
        )
    ).one()
    return [
        GaugeMetricFamily(
            "nexus_outbox_due",
            "Outbox messages due for publishing but not yet confirmed by the broker.",
            value=pending,
        ),
        GaugeMetricFamily(
            "nexus_outbox_lag_seconds",
            "How long the oldest due outbox message has been waiting; 0 when none.",
            value=float(lag),
        ),
        GaugeMetricFamily(
            "nexus_outbox_failed_permanently",
            "Outbox messages that the relay gave up on.",
            value=failed,
        ),
    ]


async def _worker_families(session: AsyncSession, settings: Settings) -> list[_Family]:
    rows = (
        await session.execute(
            text(
                "SELECT w.draining, "
                "EXTRACT(EPOCH FROM now() - w.last_heartbeat_at) <= :ttl AS fresh, "
                "w.capacity_slots, "
                "(SELECT count(*) FROM jobs j WHERE j.worker_id = w.worker_id "
                " AND j.state = 'running') AS busy "
                "FROM worker_registrations w"
            ),
            {"ttl": settings.worker_heartbeat_ttl_seconds},
        )
    ).all()
    counts = {"healthy": 0, "unhealthy": 0, "draining": 0}
    capacity = busy = 0
    for draining, fresh, slots, running in rows:
        status = "draining" if draining else ("healthy" if fresh else "unhealthy")
        counts[status] += 1
        busy += running
        if status == "healthy":
            capacity += slots
    workers = GaugeMetricFamily("nexus_workers", "Registered workers by status.", labels=["status"])
    for status, count in counts.items():
        workers.add_metric([status], count)
    return [
        workers,
        GaugeMetricFamily(
            "nexus_worker_capacity_slots", "Execution slots on healthy workers.", value=capacity
        ),
        GaugeMetricFamily("nexus_worker_busy_slots", "Jobs currently running.", value=busy),
    ]


async def _broker_families(connection: AbstractRobustConnection | None) -> list[_Family]:
    # Imported here: platform.py is the owner of the probe and its timeout.
    from nexus.api.v1.platform import _broker_depth

    depth = await _broker_depth(connection)
    out: list[_Family] = [
        GaugeMetricFamily(
            "nexus_broker_up",
            "1 when the RabbitMQ queues could be inspected.",
            value=int(depth.available),
        )
    ]
    if depth.available:
        out += [
            GaugeMetricFamily(
                "nexus_broker_ready_messages",
                "Messages waiting in nexus.jobs.",
                value=depth.ready_messages or 0,
            ),
            GaugeMetricFamily(
                "nexus_broker_consumers", "Consumers on nexus.jobs.", value=depth.consumers or 0
            ),
            GaugeMetricFamily(
                "nexus_broker_dead_letter_messages",
                "Messages held in nexus.dead-letter.",
                value=depth.dead_letter_messages or 0,
            ),
        ]
    return out


async def render_state_metrics(
    session: AsyncSession, connection: AbstractRobustConnection | None, settings: Settings
) -> bytes:
    families: list[_Family] = []
    try:
        families += await _job_families(session)
        families += await _outbox_families(session)
        families += await _worker_families(session, settings)
        database_ok = 1
    except Exception:  # noqa: BLE001 - report the failure as a metric, not a 500
        await session.rollback()
        families = []
        database_ok = 0
    families.append(
        GaugeMetricFamily(
            "nexus_state_database_up",
            "1 when the job, outbox and worker tables could be read.",
            value=database_ok,
        )
    )
    if settings.event_bus.value == "rabbitmq":
        families += await _broker_families(connection)
    registry = CollectorRegistry(auto_describe=False)
    registry.register(_Static(families))
    return generate_latest(registry)
