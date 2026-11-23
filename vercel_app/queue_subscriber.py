"""Vercel Queues push consumer entrypoint: `[[tool.vercel.subscribers]]`.

Importing the real module registers its @subscribe handler, which is what
Vercel's build-time introspection discovers."""

import vercel_app  # noqa: F401 - puts src/ on sys.path
from nexus.entrypoints.queue_subscriber import execute_dispatched_job, execute_dispatched_task

__all__ = ["execute_dispatched_job", "execute_dispatched_task"]
