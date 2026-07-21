from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from nexus.domain.worker_capabilities import WorkerCapabilities
from nexus.domain.worker_registration import WorkerRegistration, is_dead


def _registration(last_heartbeat_at: datetime) -> WorkerRegistration:
    return WorkerRegistration(
        worker_id=uuid4(),
        capabilities=WorkerCapabilities(
            cpu_cores=1.0, memory_mb=1024, architecture="x86_64", region="us-east", runtime="py"
        ),
        capacity_slots=1,
        draining=False,
        registered_at=last_heartbeat_at,
        last_heartbeat_at=last_heartbeat_at,
    )


@pytest.mark.parametrize(
    ("age", "ttl", "expected"),
    [
        (timedelta(seconds=1), timedelta(seconds=15), False),
        (timedelta(seconds=15), timedelta(seconds=15), False),  # exactly at boundary: not yet dead
        (timedelta(seconds=16), timedelta(seconds=15), True),
        (timedelta(minutes=5), timedelta(seconds=15), True),
    ],
)
def test_is_dead(age, ttl, expected):
    now = datetime.now(UTC)
    registration = _registration(last_heartbeat_at=now - age)

    assert is_dead(registration, now=now, ttl=ttl) is expected
