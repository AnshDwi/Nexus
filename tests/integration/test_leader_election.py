import asyncio
import time
from datetime import timedelta
from uuid import uuid4

import pytest

from nexus.infrastructure.coordination.leader_election import PostgresLeaderElection

pytestmark = pytest.mark.integration

_NORMAL_LEASE = timedelta(seconds=15)


async def _attempt(session_factory, replica_id, lease_name, lease_duration=_NORMAL_LEASE):
    async with session_factory() as session:
        election = PostgresLeaderElection(
            session, replica_id=replica_id, lease_name=lease_name, lease_duration=lease_duration
        )
        status = await election.try_acquire_or_renew()
        await session.commit()
        return status


async def test_first_acquisition_succeeds(leader_lease_session_factory):
    lease_name = f"test-{uuid4()}"
    replica_id = uuid4()

    status = await _attempt(leader_lease_session_factory, replica_id, lease_name)

    assert status.is_leader is True
    assert status.holder_id == replica_id
    assert status.fencing_token == 1


async def test_renewal_by_same_holder_increments_token_without_changing_holder(
    leader_lease_session_factory,
):
    lease_name = f"test-{uuid4()}"
    replica_id = uuid4()

    first = await _attempt(leader_lease_session_factory, replica_id, lease_name)
    second = await _attempt(leader_lease_session_factory, replica_id, lease_name)

    assert first.is_leader is True
    assert second.is_leader is True
    assert second.holder_id == replica_id
    assert second.fencing_token == first.fencing_token + 1


async def test_failover_after_expiry_transfers_holder_and_increments_token(
    leader_lease_session_factory,
):
    lease_name = f"test-{uuid4()}"
    replica_a, replica_b = uuid4(), uuid4()

    acquired_by_a = await _attempt(
        leader_lease_session_factory,
        replica_a,
        lease_name,
        lease_duration=timedelta(milliseconds=50),
    )
    assert acquired_by_a.is_leader is True

    await asyncio.sleep(0.15)  # let A's short lease expire

    taken_by_b = await _attempt(leader_lease_session_factory, replica_b, lease_name)

    assert taken_by_b.is_leader is True
    assert taken_by_b.holder_id == replica_b
    assert taken_by_b.fencing_token == acquired_by_a.fencing_token + 1


async def test_zombie_leader_is_rejected_indefinitely_after_failover(leader_lease_session_factory):
    lease_name = f"test-{uuid4()}"
    replica_a, replica_b = uuid4(), uuid4()

    await _attempt(
        leader_lease_session_factory,
        replica_a,
        lease_name,
        lease_duration=timedelta(milliseconds=50),
    )
    await asyncio.sleep(0.15)
    b_first = await _attempt(leader_lease_session_factory, replica_b, lease_name)

    # A, unaware it was superseded, tries to "renew" -- must be rejected.
    a_zombie_attempt = await _attempt(leader_lease_session_factory, replica_a, lease_name)
    assert a_zombie_attempt.is_leader is False
    assert a_zombie_attempt.holder_id == replica_b

    # B renews twice more; A must stay locked out, not just immediately after failover.
    await _attempt(leader_lease_session_factory, replica_b, lease_name)
    b_latest = await _attempt(leader_lease_session_factory, replica_b, lease_name)

    a_zombie_attempt_again = await _attempt(leader_lease_session_factory, replica_a, lease_name)
    assert a_zombie_attempt_again.is_leader is False
    assert a_zombie_attempt_again.holder_id == replica_b
    assert a_zombie_attempt_again.fencing_token == b_latest.fencing_token
    assert b_first.fencing_token < b_latest.fencing_token


async def test_fencing_token_is_strictly_monotonic_across_failover(leader_lease_session_factory):
    lease_name = f"test-{uuid4()}"
    replica_a, replica_b = uuid4(), uuid4()
    short_lease = timedelta(milliseconds=50)

    tokens = []
    tokens.append(
        (
            await _attempt(leader_lease_session_factory, replica_a, lease_name, short_lease)
        ).fencing_token
    )
    tokens.append(
        (
            await _attempt(leader_lease_session_factory, replica_a, lease_name, short_lease)
        ).fencing_token
    )
    await asyncio.sleep(0.15)
    tokens.append(
        (await _attempt(leader_lease_session_factory, replica_b, lease_name)).fencing_token
    )
    tokens.append(
        (await _attempt(leader_lease_session_factory, replica_b, lease_name)).fencing_token
    )

    assert all(tokens[i] < tokens[i + 1] for i in range(len(tokens) - 1)), (
        f"fencing token must be strictly increasing, got {tokens}"
    )


async def test_concurrent_acquisition_is_race_safe(leader_lease_session_factory):
    lease_name = f"test-{uuid4()}"
    replica_ids = [uuid4() for _ in range(10)]

    results = await asyncio.gather(
        *(_attempt(leader_lease_session_factory, rid, lease_name) for rid in replica_ids)
    )

    winners = [r for r in results if r.is_leader]
    assert len(winners) == 1
    assert all(r.fencing_token == winners[0].fencing_token for r in results)
    assert all(r.holder_id == winners[0].holder_id for r in results)


async def test_lease_acquisition_latency_benchmark(leader_lease_session_factory):
    lease_name = f"test-{uuid4()}"
    replica_id = uuid4()
    durations: list[float] = []

    for _ in range(50):
        start = time.perf_counter()
        await _attempt(leader_lease_session_factory, replica_id, lease_name)
        durations.append(time.perf_counter() - start)

    durations.sort()
    p50 = durations[len(durations) // 2]
    p95 = durations[int(len(durations) * 0.95)]
    print(
        f"\nlease acquire/renew latency over {len(durations)} calls: "
        f"p50={p50 * 1000:.2f}ms p95={p95 * 1000:.2f}ms"
    )

    # Generous bound against a local Dockerized Postgres -- this is a sanity
    # ceiling to catch real regressions (e.g. a missing index), not a strict
    # performance gate.
    assert p95 < 0.5
