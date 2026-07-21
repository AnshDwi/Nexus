from nexus.application.interfaces.event_publisher import EventPublisher
from nexus.application.interfaces.outbox_relay_store import OutboxRelayStore


async def relay_outbox_once(store: OutboxRelayStore, publisher: EventPublisher) -> bool:
    """Claim and dispatch a single outbox event.

    Returns True if an event was claimed and handled (published, or failure
    recorded), False if there was nothing to do. The caller owns the
    transaction boundary: a crash anywhere in this call must roll back
    everything, leaving the event unclaimed for a later retry.
    """
    event = await store.claim_next()
    if event is None:
        return False

    try:
        await publisher.publish(event)
    except Exception as exc:  # noqa: BLE001
        await store.record_failure(event.event_id, str(exc))
        return True

    await store.mark_dispatched(event.event_id)
    return True
