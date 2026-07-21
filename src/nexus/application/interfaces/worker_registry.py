from datetime import timedelta
from typing import Protocol
from uuid import UUID

from nexus.domain.worker_capabilities import WorkerCapabilities
from nexus.domain.worker_registration import WorkerRegistration


class WorkerRegistry(Protocol):
    """Storage-agnostic worker registry. PostgreSQL is one implementation,
    not an assumption baked into this contract."""

    async def register(
        self, worker_id: UUID, capabilities: WorkerCapabilities, capacity_slots: int
    ) -> WorkerRegistration:
        """Register a worker, or -- if worker_id already exists -- treat it
        as a fresh process instance: capabilities/capacity are replaced and
        draining is reset, since a restarted process is not the draining
        one that registered before."""
        ...

    async def heartbeat(self, worker_id: UUID) -> WorkerRegistration | None:
        """Renew liveness. Returns None if worker_id was never registered
        (or was since deregistered) -- the caller should re-register."""
        ...

    async def update_capabilities(
        self, worker_id: UUID, capabilities: WorkerCapabilities
    ) -> None: ...

    async def start_draining(self, worker_id: UUID) -> None: ...

    async def deregister(self, worker_id: UUID) -> None: ...

    async def list_dead_workers(self, ttl: timedelta) -> list[WorkerRegistration]:
        """Workers whose last heartbeat is older than ttl. Detection only --
        the caller decides what to do about it (e.g. deregister, requeue its
        in-flight work)."""
        ...
