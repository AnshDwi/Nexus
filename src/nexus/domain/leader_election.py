from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(frozen=True, slots=True)
class LeaseStatus:
    """The current truth of a lease, regardless of who's asking.

    `is_leader` is relative to the caller (holder_id == the caller's replica
    id); `holder_id`/`fencing_token`/`expires_at` always reflect the lease's
    actual current state so a caller that lost leadership can still see who
    holds it now.
    """

    is_leader: bool
    holder_id: UUID
    fencing_token: int
    expires_at: datetime


def classify_transition(*, was_leader: bool, is_leader: bool) -> str | None:
    """Pure classification of a before/after leadership pair.

    Returns "acquired", "lost", or None (steady state -- still/still-not
    leader, no transition to report).
    """
    if was_leader and not is_leader:
        return "lost"
    if not was_leader and is_leader:
        return "acquired"
    return None
