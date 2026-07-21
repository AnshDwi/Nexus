# ADR 0003: Scheduler Leader Election via Fencing-Token Lease

## Status

Accepted (implemented in M3 Phase 1). Builds on the leader-election design approved during Act 2's architecture review, and directly addresses the naive-lock critique flagged as deferred work in [ADR 0002](0002-transactional-outbox-pattern.md).

## Context

The scheduler leader (built in later M3 phases) makes exclusive decisions -- DAG readiness, dispatch ordering, admission control -- that must never be made by two replicas simultaneously. A bare distributed lock is not sufficient: Martin Kleppmann's well-known critique of naive lock-based leader election applies directly here -- a replica that pauses (GC, scheduler hiccup, network partition) can wake up still believing it holds the lock and issue writes after a new leader has already taken over, corrupting state. The fix is a fencing token: a monotonically increasing number that lets any downstream system reject writes from a leader that has been superseded, even if that leader doesn't know it yet.

## Decision

Leader election is exposed as a storage-agnostic port (`LeaderElection.try_acquire_or_renew`); PostgreSQL is the only implementation today, but nothing in the port assumes it. `PostgresLeaderElection` implements it with a single atomic statement:

```sql
INSERT INTO scheduler_leader_lease (lease_name, holder_id, fencing_token, expires_at)
VALUES (:lease_name, :self_id, 1, :new_expiry)
ON CONFLICT (lease_name) DO UPDATE SET
    holder_id = :self_id,
    fencing_token = scheduler_leader_lease.fencing_token + 1,
    expires_at = :new_expiry
WHERE scheduler_leader_lease.holder_id = :self_id
   OR scheduler_leader_lease.expires_at < :now
RETURNING holder_id, fencing_token, expires_at;
```

This single statement handles first-ever acquisition, renewal by the existing holder, and failover to a new holder after expiry -- and it self-initializes (no seed migration needed, unlike the original design sketch). The `WHERE` clause on the `DO UPDATE` action is what makes zombie rejection automatic: if a caller is neither the current holder nor facing an expired lease, Postgres leaves the row untouched and returns nothing. There is no window where the fencing token can be read, incremented in application code, and written back non-atomically -- the increment happens inside the same statement that decides who wins.

An earlier, purely informational `SELECT` runs before the statement to classify the outcome (acquired / renewed / lost / no-op) for logging and metrics. This is explicitly **not** a correctness dependency -- leadership truth comes only from whether the atomic statement returned a row. A race between the informational `SELECT` and the real statement can at worst mislabel a metric, never the actual leadership decision.

## Consequences

- Zombie leader rejection is proven, not assumed: `test_zombie_leader_is_rejected_indefinitely_after_failover` shows a superseded replica staying locked out even after the new leader has renewed multiple times, not just immediately after failover.
- Strict fencing-token monotonicity is proven under both sequential failover and a 10-way concurrent acquisition race (`test_concurrent_acquisition_is_race_safe`, mirroring the concurrency proofs already established for the Inbox and Outbox kernels in M1/M2).
- Acquisition latency was benchmarked, not assumed: p50 ≈ 8.5ms, p95 ≈ 11.4ms over 50 calls against a local Dockerized Postgres (two round trips -- the informational read plus the atomic write -- both indexed on the primary key).
- The port stays storage-agnostic on purpose: a future Redis- or etcd-backed implementation is possible without touching any caller, though none is planned.
- What this milestone does *not* yet do: propagate the fencing token into downstream writes (e.g., tagging future task-dispatch rows so a zombie leader's stale writes are rejected at the point of use). That mechanism is scoped to the phase that actually produces those writes (Scheduler Core, M3 Phase 5) rather than built speculatively now.
