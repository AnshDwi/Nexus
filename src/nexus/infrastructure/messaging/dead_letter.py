"""Housekeeping for the broker's `nexus.dead-letter` queue.

When a job exhausts its attempts, the worker rejects the dispatch message
into `nexus.dead-letter` and the job row gets `dead_lettered_at`. If an
operator later retries the job, the row is updated but the broker message
stays behind, so the broker count drifts above the number of jobs that are
actually dead-lettered.

`prune_resolved_dead_letters` removes exactly those leftovers: a message is
dropped only when it is a job dispatch whose job exists and is no longer
dead-lettered *for that attempt*. Everything else is put back untouched:
messages for jobs that are still dead-lettered, task messages, poison
messages that cannot be parsed, and messages for unknown jobs. The job's
full history stays in `job_transitions` / `job_executions`.

Messages are fetched with `basic.get` and held unacknowledged until the
decision is made, so RabbitMQ will not hand the same message out twice in
one pass, and two replicas pruning at once see disjoint messages. Closing
the channel returns anything not yet settled.
"""

from __future__ import annotations

import contextlib
import json
import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from aio_pika.abc import AbstractIncomingMessage, AbstractRobustConnection
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.infrastructure.messaging.rabbitmq_topology import DEAD_LETTER_QUEUE
from nexus.infrastructure.observability.metrics import dead_letter_pruned_total

logger = logging.getLogger("nexus.dead_letter")


@dataclass(frozen=True, slots=True)
class PruneReport:
    inspected: int = 0
    pruned: int = 0
    kept: int = 0


def _job_reference(message: AbstractIncomingMessage) -> tuple[UUID, int] | None:
    """(job_id, attempt) for a job dispatch message, else None."""
    try:
        data: dict[str, Any] = json.loads(message.body)
        if data.get("aggregate_type") != "job":
            return None
        payload = data.get("payload") or {}
        job_id = UUID(str(payload.get("job_id") or data["aggregate_id"]))
        attempt = int(payload["attempt"])
    except (ValueError, KeyError, TypeError):
        return None
    return job_id, attempt


async def prune_resolved_dead_letters(
    connection: AbstractRobustConnection,
    session_factory: async_sessionmaker[AsyncSession],
    *,
    limit: int = 200,
) -> PruneReport:
    channel = await connection.channel()
    held: list[AbstractIncomingMessage] = []
    try:
        queue = await channel.declare_queue(DEAD_LETTER_QUEUE, passive=True)
        for _ in range(limit):
            message = await queue.get(no_ack=False, fail=False)
            if message is None:
                break
            held.append(message)
        if not held:
            return PruneReport()

        refs = {id(m): _job_reference(m) for m in held}
        job_ids = list({ref[0] for ref in refs.values() if ref is not None})
        current: dict[UUID, tuple[int, bool]] = {}
        if job_ids:
            async with session_factory() as session:
                rows = await session.execute(
                    text(
                        "SELECT job_id, attempt, dead_lettered_at IS NOT NULL "
                        "FROM jobs WHERE job_id = ANY(:ids)"
                    ),
                    {"ids": job_ids},
                )
                current = {job_id: (attempt, dead) for job_id, attempt, dead in rows}

        pruned = kept = 0
        for message in held:
            ref = refs[id(message)]
            state = current.get(ref[0]) if ref else None
            resolved = (
                ref is not None
                and state is not None
                and not (state[1] and state[0] == ref[1])  # still dead-lettered at this attempt
            )
            if resolved:
                await message.ack()
                pruned += 1
            else:
                await message.nack(requeue=True)
                kept += 1
        if pruned:
            dead_letter_pruned_total.inc(pruned)
            logger.info(
                "pruned resolved dead-letter messages", extra={"pruned": pruned, "kept": kept}
            )
        return PruneReport(inspected=len(held), pruned=pruned, kept=kept)
    finally:
        with contextlib.suppress(Exception):
            await channel.close()  # returns anything still unacknowledged
