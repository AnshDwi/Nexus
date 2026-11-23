"""How long a scheduler replica has been able to watch worker heartbeats.

A worker is declared dead when its last heartbeat is older than the TTL. That
conclusion is only sound if heartbeats *could* have been written and read
during that TTL. After a PostgreSQL outage nobody could: the workers'
heartbeat writes failed and the scheduler could not read them, so every
worker looks stale the moment the database returns, even ones that are alive
and still running jobs. Sweeping them then re-runs their jobs while the
originals are still executing (issue C1).

The window restarts whenever the replica's view may have been interrupted:

* a maintenance pass failed (the database, or anything else, was unavailable);
* the replica is not the leader, or was fenced out;
* the replica has just (re)acquired leadership -- including renewing its own
  lease after it had lapsed;
* the gap since its previous leader pass exceeded the leader lease duration
  (its lease lapsed in between, even if no error was seen).

It opens at the first successful leader pass after a restart, and the
dead-worker sweep waits until it spans a full heartbeat TTL. Live workers
get one TTL of working database access to heartbeat again; a worker that
is really dead is still detected, one TTL later than before. Nothing else
waits: lease-expiry recovery, redispatch, task reconciliation and the
outbox relay run as usual.

One instance per scheduler replica, in memory, used by that replica's single
maintenance loop (passes never overlap within a replica). Replicas do not
share it: a replica that becomes leader starts with an empty window.
"""

import time
from collections.abc import Callable


class HeartbeatObservationWindow:
    def __init__(
        self,
        *,
        ttl_seconds: float,
        lease_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._ttl = ttl_seconds
        self._lease = lease_seconds
        self._clock = clock
        self._since: float | None = None
        self._last_pass: float | None = None
        self.last_reset_reason: str | None = "not started"

    def reset(self, reason: str) -> None:
        self._since = None
        self._last_pass = None
        self.last_reset_reason = reason

    def begin_leader_pass(self, *, newly_acquired: bool) -> None:
        """Call once per maintenance pass, right after the lease was acquired
        or renewed (so the database was reachable at this moment)."""
        now = self._clock()
        if newly_acquired:
            self.reset("leadership acquired")
        elif self._last_pass is not None and now - self._last_pass > self._lease:
            self.reset("leader lease lapsed between passes")
        if self._since is None:
            self._since = now
        self._last_pass = now

    @property
    def observed_seconds(self) -> float:
        if self._since is None:
            return 0.0
        return max(self._clock() - self._since, 0.0)

    @property
    def allows_dead_worker_sweep(self) -> bool:
        return self._since is not None and self.observed_seconds >= self._ttl
