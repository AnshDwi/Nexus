from typing import Protocol

from nexus.domain.leader_election import LeaseStatus


class LeaderElection(Protocol):
    """Storage-agnostic leader election. PostgreSQL is one implementation,
    not an assumption baked into this contract."""

    async def try_acquire_or_renew(self) -> LeaseStatus:
        """Atomically acquire the lease if unheld/expired, or renew it if
        this replica already holds it. Always returns the lease's true
        current state, whether or not the caller is the holder."""
        ...
