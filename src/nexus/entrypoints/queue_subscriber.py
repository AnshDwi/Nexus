"""Vercel Queues push-mode consumer for task dispatch messages.

Declared in pyproject.toml under [[tool.vercel.subscribers]]; at build time
Vercel imports this module, reads the @subscribe registration and generates a
private, queue-triggered function (not reachable from the internet).

Acknowledgement semantics (Vercel Queues Python SDK):
  * handler returns           -> message acknowledged
  * handler raises RetryAfter -> redelivered after the given delay
  * handler raises anything   -> redelivered after `retry_after`; this covers
    database outages and crashes, the attempt-guarded claim makes the
    redelivery safe.
"""

import logging
import os
from typing import Any

from vercel.queue import Message, RetryAfter, subscribe

from nexus.entrypoints.runtime import get_worker_runtime
from nexus.infrastructure.execution.executor import DispatchMessage, RetryLater

logger = logging.getLogger("nexus.queue_subscriber")

# Read from the environment, not Settings: Vercel imports this module at
# build time to discover the subscription, before runtime secrets matter.
DISPATCH_TOPIC = os.environ.get("VERCEL_QUEUE_DISPATCH_TOPIC", "nexus-task-dispatch")
CONSUMER_GROUP = "nexus-executor"


@subscribe(topic=DISPATCH_TOPIC, consumer_group=CONSUMER_GROUP, retry_after=15)
async def execute_dispatched_task(message: Message[dict[str, Any]]) -> None:
    try:
        dispatch = DispatchMessage.from_payload(message.payload)
    except (KeyError, TypeError, ValueError):
        # A malformed message can never succeed; acknowledge it (by
        # returning) rather than letting it retry until it expires.
        logger.error(
            "discarding malformed dispatch message",
            extra={"message_id": str(message.message_id)},
        )
        return
    runtime = await get_worker_runtime()
    try:
        await runtime.executor.handle_dispatch(dispatch)
    except RetryLater as deferred:
        raise RetryAfter(max(1, int(deferred.delay.total_seconds()))) from None
