"""RabbitMQ topology for NEXUS (declared idempotently by publishers and
workers on connect).

    exchange nexus.events (topic, durable)
      ├─ job.dispatch_requested  ─► queue nexus.jobs   (priority 0-10, DLX)
      └─ task.dispatch_requested ─► queue nexus.tasks  (DLX)
    exchange nexus.dlx (topic, durable)
      └─ #                        ─► queue nexus.dead-letter

Work queues are durable classic queues with x-max-priority so CRITICAL jobs
overtake LOW ones. Messages a worker rejects (poison messages, jobs that
exhausted their retries) are dead-lettered to nexus.dead-letter for
inspection instead of being dropped.
"""

from dataclasses import dataclass
from typing import Any

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractQueue

EVENTS_EXCHANGE = "nexus.events"
DEAD_LETTER_EXCHANGE = "nexus.dlx"
JOBS_QUEUE = "nexus.jobs"
TASKS_QUEUE = "nexus.tasks"
DEAD_LETTER_QUEUE = "nexus.dead-letter"
JOB_DISPATCH_ROUTING_KEY = "job.dispatch_requested"
TASK_DISPATCH_ROUTING_KEY = "task.dispatch_requested"
WORK_ROUTING_KEYS = frozenset({JOB_DISPATCH_ROUTING_KEY, TASK_DISPATCH_ROUTING_KEY})
MAX_PRIORITY = 10


@dataclass
class Topology:
    events: AbstractExchange
    jobs: AbstractQueue
    tasks: AbstractQueue
    dead_letter: AbstractQueue


async def declare_topology(channel: AbstractChannel) -> Topology:
    events = await channel.declare_exchange(
        EVENTS_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )
    dlx = await channel.declare_exchange(
        DEAD_LETTER_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )
    dead_letter = await channel.declare_queue(DEAD_LETTER_QUEUE, durable=True)
    await dead_letter.bind(dlx, routing_key="#")

    work_args: dict[str, Any] = {"x-dead-letter-exchange": DEAD_LETTER_EXCHANGE}
    jobs = await channel.declare_queue(
        JOBS_QUEUE, durable=True, arguments={**work_args, "x-max-priority": MAX_PRIORITY}
    )
    await jobs.bind(events, routing_key=JOB_DISPATCH_ROUTING_KEY)
    tasks = await channel.declare_queue(TASKS_QUEUE, durable=True, arguments=work_args)
    await tasks.bind(events, routing_key=TASK_DISPATCH_ROUTING_KEY)
    return Topology(events=events, jobs=jobs, tasks=tasks, dead_letter=dead_letter)
