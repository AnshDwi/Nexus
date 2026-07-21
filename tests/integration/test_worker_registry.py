import asyncio
import time
from datetime import timedelta
from uuid import uuid4

import pytest

from nexus.domain.worker_capabilities import WorkerCapabilities
from nexus.infrastructure.coordination.worker_registry import PostgresWorkerRegistry
from nexus.infrastructure.observability.metrics import (
    scheduler_worker_registrations_total,
)

pytestmark = pytest.mark.integration


def _capabilities(**overrides) -> WorkerCapabilities:
    defaults = {
        "cpu_cores": 4.0,
        "memory_mb": 8192,
        "architecture": "x86_64",
        "region": "us-east",
        "runtime": "python3.12",
    }
    defaults.update(overrides)
    return WorkerCapabilities(**defaults)


async def _register(session_factory, worker_id, capabilities=None, capacity_slots=2):
    async with session_factory() as session:
        registry = PostgresWorkerRegistry(session)
        registration = await registry.register(
            worker_id, capabilities or _capabilities(), capacity_slots
        )
        await session.commit()
        return registration


async def _heartbeat(session_factory, worker_id):
    async with session_factory() as session:
        registry = PostgresWorkerRegistry(session)
        registration = await registry.heartbeat(worker_id)
        await session.commit()
        return registration


async def test_register_creates_new_worker(worker_registry_session_factory):
    worker_id = uuid4()

    registration = await _register(worker_registry_session_factory, worker_id, capacity_slots=3)

    assert registration.worker_id == worker_id
    assert registration.capacity_slots == 3
    assert registration.draining is False


async def test_register_existing_worker_is_treated_as_restart(worker_registry_session_factory):
    worker_id = uuid4()
    before = scheduler_worker_registrations_total.labels(event="restarted")._value.get()

    first = await _register(worker_registry_session_factory, worker_id, capacity_slots=2)
    async with worker_registry_session_factory() as session:
        await PostgresWorkerRegistry(session).start_draining(worker_id)
        await session.commit()

    restarted = await _register(
        worker_registry_session_factory,
        worker_id,
        capabilities=_capabilities(memory_mb=16384),
        capacity_slots=4,
    )

    after = scheduler_worker_registrations_total.labels(event="restarted")._value.get()
    assert after == before + 1
    assert restarted.worker_id == worker_id
    assert restarted.capacity_slots == 4
    assert restarted.capabilities.memory_mb == 16384
    assert restarted.draining is False  # a fresh process instance is not the draining one
    assert restarted.registered_at >= first.registered_at


async def test_heartbeat_updates_last_heartbeat_at(worker_registry_session_factory):
    worker_id = uuid4()
    registered = await _register(worker_registry_session_factory, worker_id)

    await asyncio.sleep(0.05)
    heartbeat = await _heartbeat(worker_registry_session_factory, worker_id)

    assert heartbeat is not None
    assert heartbeat.last_heartbeat_at > registered.last_heartbeat_at


async def test_heartbeat_for_unknown_worker_returns_none(worker_registry_session_factory):
    result = await _heartbeat(worker_registry_session_factory, uuid4())
    assert result is None


async def test_concurrent_heartbeats_are_safe(worker_registry_session_factory):
    worker_id = uuid4()
    await _register(worker_registry_session_factory, worker_id)

    results = await asyncio.gather(
        *(_heartbeat(worker_registry_session_factory, worker_id) for _ in range(10))
    )

    assert all(r is not None for r in results)
    assert all(r.worker_id == worker_id for r in results)


async def test_stale_worker_is_detected_as_dead(worker_registry_session_factory):
    worker_id = uuid4()
    await _register(worker_registry_session_factory, worker_id)
    await asyncio.sleep(0.1)

    async with worker_registry_session_factory() as session:
        dead = await PostgresWorkerRegistry(session).list_dead_workers(timedelta(milliseconds=20))

    assert worker_id in [w.worker_id for w in dead]


async def test_active_worker_not_detected_as_dead(worker_registry_session_factory):
    worker_id = uuid4()
    await _register(worker_registry_session_factory, worker_id)

    async with worker_registry_session_factory() as session:
        dead = await PostgresWorkerRegistry(session).list_dead_workers(timedelta(seconds=15))

    assert worker_id not in [w.worker_id for w in dead]


async def test_dead_worker_detection_returns_multiple_stale_workers(
    worker_registry_session_factory,
):
    # Wide margins on both sides: stale workers must be well past the TTL, and
    # the fresh worker's own registration round-trip must be well under it --
    # a too-tight TTL here is a flaky test, not a correctness signal.
    stale_a, stale_b, fresh = uuid4(), uuid4(), uuid4()
    await _register(worker_registry_session_factory, stale_a)
    await _register(worker_registry_session_factory, stale_b)
    await asyncio.sleep(0.3)
    await _register(worker_registry_session_factory, fresh)

    async with worker_registry_session_factory() as session:
        dead = await PostgresWorkerRegistry(session).list_dead_workers(timedelta(milliseconds=150))

    dead_ids = {w.worker_id for w in dead}
    assert dead_ids == {stale_a, stale_b}


async def test_graceful_deregistration_removes_worker(worker_registry_session_factory):
    worker_id = uuid4()
    await _register(worker_registry_session_factory, worker_id)

    async with worker_registry_session_factory() as session:
        await PostgresWorkerRegistry(session).deregister(worker_id)
        await session.commit()

    result = await _heartbeat(worker_registry_session_factory, worker_id)
    assert result is None


async def test_capability_update_persists(worker_registry_session_factory):
    worker_id = uuid4()
    await _register(worker_registry_session_factory, worker_id)

    async with worker_registry_session_factory() as session:
        registry = PostgresWorkerRegistry(session)
        await registry.update_capabilities(worker_id, _capabilities(region="eu-west"))
        await session.commit()

    updated = await _heartbeat(worker_registry_session_factory, worker_id)
    assert updated is not None
    assert updated.capabilities.region == "eu-west"


async def test_heartbeat_latency_benchmark(worker_registry_session_factory):
    worker_id = uuid4()
    await _register(worker_registry_session_factory, worker_id)

    durations: list[float] = []
    for _ in range(50):
        start = time.perf_counter()
        await _heartbeat(worker_registry_session_factory, worker_id)
        durations.append(time.perf_counter() - start)

    durations.sort()
    p50 = durations[len(durations) // 2]
    p95 = durations[int(len(durations) * 0.95)]
    print(
        f"\nheartbeat latency over {len(durations)} calls: "
        f"p50={p50 * 1000:.2f}ms p95={p95 * 1000:.2f}ms"
    )

    # Generous sanity ceiling against a local Dockerized Postgres, not a strict gate.
    assert p95 < 0.5
