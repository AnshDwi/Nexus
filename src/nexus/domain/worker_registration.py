from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

from nexus.domain.worker_capabilities import WorkerCapabilities


@dataclass(frozen=True, slots=True)
class WorkerRegistration:
    worker_id: UUID
    capabilities: WorkerCapabilities
    capacity_slots: int
    draining: bool
    registered_at: datetime
    last_heartbeat_at: datetime


def is_dead(registration: WorkerRegistration, *, now: datetime, ttl: timedelta) -> bool:
    """Pure liveness check. 'Dead' is derived from the heartbeat timestamp,
    not stored as a separate status -- one less thing that can drift out of
    sync with reality."""
    return now - registration.last_heartbeat_at > ttl
