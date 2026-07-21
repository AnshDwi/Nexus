import logging
import time
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import Row, delete, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.worker_capabilities import GpuCapability, WorkerCapabilities
from nexus.domain.worker_registration import WorkerRegistration
from nexus.infrastructure.db.models.worker_registration import WorkerRegistrationModel
from nexus.infrastructure.observability.metrics import (
    scheduler_dead_workers_detected_total,
    scheduler_worker_heartbeat_latency_seconds,
    scheduler_worker_heartbeats_total,
    scheduler_worker_registrations_total,
    scheduler_worker_registry_failures_total,
    scheduler_worker_state_transitions_total,
)

logger = logging.getLogger("nexus.scheduler.worker_registry")

_RETURNING_COLUMNS = (
    WorkerRegistrationModel.worker_id,
    WorkerRegistrationModel.capabilities,
    WorkerRegistrationModel.capacity_slots,
    WorkerRegistrationModel.draining,
    WorkerRegistrationModel.registered_at,
    WorkerRegistrationModel.last_heartbeat_at,
)


def _capabilities_to_dict(capabilities: WorkerCapabilities) -> dict[str, Any]:
    return {
        "cpu_cores": capabilities.cpu_cores,
        "memory_mb": capabilities.memory_mb,
        "architecture": capabilities.architecture,
        "region": capabilities.region,
        "runtime": capabilities.runtime,
        "gpu": (
            {
                "model": capabilities.gpu.model,
                "count": capabilities.gpu.count,
                "memory_mb": capabilities.gpu.memory_mb,
            }
            if capabilities.gpu is not None
            else None
        ),
        "custom": capabilities.custom,
    }


def _capabilities_from_dict(data: dict[str, Any]) -> WorkerCapabilities:
    gpu_data = data.get("gpu")
    return WorkerCapabilities(
        cpu_cores=data["cpu_cores"],
        memory_mb=data["memory_mb"],
        architecture=data["architecture"],
        region=data["region"],
        runtime=data["runtime"],
        gpu=(
            GpuCapability(
                model=gpu_data["model"],
                count=gpu_data["count"],
                memory_mb=gpu_data["memory_mb"],
            )
            if gpu_data is not None
            else None
        ),
        custom=data.get("custom", {}),
    )


def _row_to_registration(row: Row[Any]) -> WorkerRegistration:
    worker_id, capabilities, capacity_slots, draining, registered_at, last_heartbeat_at = row
    return WorkerRegistration(
        worker_id=worker_id,
        capabilities=_capabilities_from_dict(capabilities),
        capacity_slots=capacity_slots,
        draining=draining,
        registered_at=registered_at,
        last_heartbeat_at=last_heartbeat_at,
    )


class PostgresWorkerRegistry:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def register(
        self, worker_id: UUID, capabilities: WorkerCapabilities, capacity_slots: int
    ) -> WorkerRegistration:
        try:
            now = datetime.now(UTC)
            existed = (
                await self._session.execute(
                    select(WorkerRegistrationModel.worker_id).where(
                        WorkerRegistrationModel.worker_id == worker_id
                    )
                )
            ).first() is not None

            capabilities_json = _capabilities_to_dict(capabilities)
            stmt = (
                pg_insert(WorkerRegistrationModel)
                .values(
                    worker_id=worker_id,
                    capabilities=capabilities_json,
                    capacity_slots=capacity_slots,
                    draining=False,
                    registered_at=now,
                    last_heartbeat_at=now,
                )
                .on_conflict_do_update(
                    index_elements=[WorkerRegistrationModel.worker_id],
                    set_={
                        "capabilities": capabilities_json,
                        "capacity_slots": capacity_slots,
                        "draining": False,
                        "registered_at": now,
                        "last_heartbeat_at": now,
                    },
                )
                .returning(*_RETURNING_COLUMNS)
            )
            row = (await self._session.execute(stmt)).one()
            registration = _row_to_registration(row)

            event = "restarted" if existed else "new"
            scheduler_worker_registrations_total.labels(event=event).inc()
            logger.info(
                "worker registered",
                extra={
                    "worker_id": str(worker_id),
                    "event": event,
                    "capacity_slots": capacity_slots,
                },
            )
            return registration
        except Exception as exc:
            scheduler_worker_registry_failures_total.labels(
                operation="register", reason=type(exc).__name__
            ).inc()
            logger.error(
                "worker registration failed", extra={"worker_id": str(worker_id)}, exc_info=exc
            )
            raise

    async def heartbeat(self, worker_id: UUID) -> WorkerRegistration | None:
        start = time.perf_counter()
        try:
            stmt = (
                update(WorkerRegistrationModel)
                .where(WorkerRegistrationModel.worker_id == worker_id)
                .values(last_heartbeat_at=datetime.now(UTC))
                .returning(*_RETURNING_COLUMNS)
            )
            row = (await self._session.execute(stmt)).first()
            if row is None:
                scheduler_worker_heartbeats_total.labels(result="unknown_worker").inc()
                logger.warning("heartbeat for unknown worker", extra={"worker_id": str(worker_id)})
                return None
            scheduler_worker_heartbeats_total.labels(result="ok").inc()
            logger.debug("worker heartbeat", extra={"worker_id": str(worker_id)})
            return _row_to_registration(row)
        except Exception as exc:
            scheduler_worker_registry_failures_total.labels(
                operation="heartbeat", reason=type(exc).__name__
            ).inc()
            logger.error("heartbeat failed", extra={"worker_id": str(worker_id)}, exc_info=exc)
            raise
        finally:
            scheduler_worker_heartbeat_latency_seconds.observe(time.perf_counter() - start)

    async def update_capabilities(self, worker_id: UUID, capabilities: WorkerCapabilities) -> None:
        try:
            await self._session.execute(
                update(WorkerRegistrationModel)
                .where(WorkerRegistrationModel.worker_id == worker_id)
                .values(capabilities=_capabilities_to_dict(capabilities))
            )
            logger.info("worker capabilities updated", extra={"worker_id": str(worker_id)})
        except Exception as exc:
            scheduler_worker_registry_failures_total.labels(
                operation="update_capabilities", reason=type(exc).__name__
            ).inc()
            logger.error(
                "capability update failed", extra={"worker_id": str(worker_id)}, exc_info=exc
            )
            raise

    async def start_draining(self, worker_id: UUID) -> None:
        try:
            await self._session.execute(
                update(WorkerRegistrationModel)
                .where(WorkerRegistrationModel.worker_id == worker_id)
                .values(draining=True)
            )
            scheduler_worker_state_transitions_total.labels(transition="drained").inc()
            logger.info("worker draining", extra={"worker_id": str(worker_id)})
        except Exception as exc:
            scheduler_worker_registry_failures_total.labels(
                operation="start_draining", reason=type(exc).__name__
            ).inc()
            logger.error("start_draining failed", extra={"worker_id": str(worker_id)}, exc_info=exc)
            raise

    async def deregister(self, worker_id: UUID) -> None:
        try:
            await self._session.execute(
                delete(WorkerRegistrationModel).where(
                    WorkerRegistrationModel.worker_id == worker_id
                )
            )
            scheduler_worker_state_transitions_total.labels(transition="deregistered").inc()
            logger.info("worker deregistered", extra={"worker_id": str(worker_id)})
        except Exception as exc:
            scheduler_worker_registry_failures_total.labels(
                operation="deregister", reason=type(exc).__name__
            ).inc()
            logger.error("deregister failed", extra={"worker_id": str(worker_id)}, exc_info=exc)
            raise

    async def list_dead_workers(self, ttl: timedelta) -> list[WorkerRegistration]:
        try:
            cutoff = datetime.now(UTC) - ttl
            rows: Sequence[Row[Any]] = (
                await self._session.execute(
                    select(*_RETURNING_COLUMNS).where(
                        WorkerRegistrationModel.last_heartbeat_at < cutoff
                    )
                )
            ).all()
            dead = [_row_to_registration(row) for row in rows]
            if dead:
                scheduler_dead_workers_detected_total.inc(len(dead))
                logger.warning(
                    "dead workers detected",
                    extra={
                        "count": len(dead),
                        "worker_ids": [str(w.worker_id) for w in dead],
                    },
                )
            return dead
        except Exception as exc:
            scheduler_worker_registry_failures_total.labels(
                operation="list_dead_workers", reason=type(exc).__name__
            ).inc()
            logger.error("list_dead_workers failed", exc_info=exc)
            raise
