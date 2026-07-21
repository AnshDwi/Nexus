import asyncio

import aio_pika
from aio_pika.abc import AbstractRobustConnection
from fastapi import Request

_MAX_CONNECT_ATTEMPTS = 5
_INITIAL_RETRY_DELAY_SECONDS = 1.0


async def create_rabbitmq_connection(rabbitmq_url: str) -> AbstractRobustConnection:
    """Connect with bounded retry/backoff.

    A broker healthcheck (e.g. Docker Compose's) can report healthy before the
    AMQP listener is actually accepting connections, and connect_robust fails
    fast on its first attempt. Retrying here makes startup resilient to that
    race regardless of orchestrator.
    """
    delay = _INITIAL_RETRY_DELAY_SECONDS
    for _ in range(_MAX_CONNECT_ATTEMPTS - 1):
        try:
            return await aio_pika.connect_robust(rabbitmq_url)
        except aio_pika.exceptions.AMQPConnectionError:
            await asyncio.sleep(delay)
            delay *= 2
    return await aio_pika.connect_robust(rabbitmq_url)


def get_rabbitmq_connection(request: Request) -> AbstractRobustConnection:
    connection: AbstractRobustConnection = request.app.state.container.rabbitmq_connection
    return connection
