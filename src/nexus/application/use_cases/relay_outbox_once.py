from nexus.application.interfaces.event_publisher import EventPublisher
from nexus.application.interfaces.outbox_relay_hook import OutboxRelayHook
from nexus.application.interfaces.outbox_relay_store import OutboxRelayStore


async def relay_outbox_once(
    store: OutboxRelayStore,
    publisher: EventPublisher,
    hook: OutboxRelayHook | None = None,
) -> bool:
    """Claim and dispatch a single outbox event.

    Returns True if an event was claimed and handled (published, skipped as
    obsolete, or failure recorded), False if there was nothing to do. The
    caller owns the transaction boundary: a crash anywhere in this call must
    roll back everything, leaving the event unclaimed for a later retry.

    `publisher.publish` must only return once the broker has accepted the
    message (publisher confirms); `hook.after_publish` relies on that.
    """
    event = await store.claim_next()
    if event is None:
        return False

    if hook is not None and not await hook.before_publish(event):
        await store.mark_dispatched(event.event_id)
        return True

    try:
        await publisher.publish(event)
    except Exception as exc:  # noqa: BLE001
        await store.record_failure(event.event_id, str(exc))
        return True

    await store.mark_dispatched(event.event_id)
    if hook is not None:
        await hook.after_publish(event)
    return True
