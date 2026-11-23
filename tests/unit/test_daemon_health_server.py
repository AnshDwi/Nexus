"""The daemons' private HTTP port: liveness probe and process metrics."""

from __future__ import annotations

import asyncio
import socket

from nexus.daemon import Heartbeat, serve_health
from nexus.infrastructure.observability.metrics import job_executions_total


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def _get(port: int, path: str) -> tuple[str, bytes]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
    await writer.drain()
    raw = await reader.read()
    writer.close()
    head, _, body = raw.partition(b"\r\n\r\n")
    return head.split(b"\r\n")[0].decode(), body


async def test_health_reflects_heartbeat_and_metrics_are_served() -> None:
    port = _free_port()
    heartbeat = Heartbeat(stall_after_seconds=60)
    job_executions_total.labels(job_type="echo", outcome="completed").inc()
    server = await serve_health(heartbeat, port)
    try:
        status, _ = await _get(port, "/health")
        assert status.endswith("200 OK")

        status, body = await _get(port, "/metrics")
        assert status.endswith("200 OK")
        assert b'nexus_job_executions_total{job_type="echo",outcome="completed"}' in body

        status, _ = await _get(port, "/other")
        assert "503" in status

        stalled = Heartbeat(stall_after_seconds=0)
        server.close()
        await server.wait_closed()
        server = await serve_health(stalled, port)
        status, _ = await _get(port, "/health")
        assert "503" in status
    finally:
        server.close()
        await server.wait_closed()
