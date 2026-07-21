from unittest.mock import AsyncMock

from nexus.infrastructure.cache.redis import get_redis
from nexus.infrastructure.db.session import get_db_session
from nexus.infrastructure.messaging.rabbitmq import get_rabbitmq_connection
from nexus.main import app


class _FakeSession:
    def __init__(self, *, should_fail: bool = False) -> None:
        self._should_fail = should_fail

    async def execute(self, *_args, **_kwargs):
        if self._should_fail:
            raise ConnectionError("postgres unreachable")
        return None


def _override_db_session(*, should_fail: bool = False):
    async def _get_session():
        yield _FakeSession(should_fail=should_fail)

    return _get_session


def _fake_redis(*, should_fail: bool = False) -> AsyncMock:
    redis = AsyncMock()
    if should_fail:
        redis.ping = AsyncMock(side_effect=ConnectionError("redis unreachable"))
    else:
        redis.ping = AsyncMock(return_value=True)
    return redis


def _fake_rabbitmq_connection(*, should_fail: bool = False) -> AsyncMock:
    connection = AsyncMock()
    if should_fail:
        connection.channel = AsyncMock(side_effect=ConnectionError("rabbitmq unreachable"))
    else:
        connection.channel = AsyncMock(return_value=AsyncMock())
    return connection


def _apply_overrides(
    *, db_fails: bool = False, redis_fails: bool = False, rabbitmq_fails: bool = False
) -> None:
    app.dependency_overrides[get_db_session] = _override_db_session(should_fail=db_fails)
    app.dependency_overrides[get_redis] = lambda: _fake_redis(should_fail=redis_fails)
    app.dependency_overrides[get_rabbitmq_connection] = lambda: _fake_rabbitmq_connection(
        should_fail=rabbitmq_fails
    )


async def test_ready_reports_ok_when_all_dependencies_healthy(client):
    _apply_overrides()

    response = await client.get("/api/v1/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "postgres": "ok",
        "redis": "ok",
        "rabbitmq": "ok",
    }


async def test_ready_reports_degraded_when_redis_unavailable(client):
    _apply_overrides(redis_fails=True)

    response = await client.get("/api/v1/ready")

    body = response.json()
    assert response.status_code == 200
    assert body["status"] == "degraded"
    assert body["redis"].startswith("error")
    assert body["postgres"] == "ok"


async def test_ready_reports_degraded_when_postgres_unavailable(client):
    _apply_overrides(db_fails=True)

    response = await client.get("/api/v1/ready")

    body = response.json()
    assert response.status_code == 200
    assert body["status"] == "degraded"
    assert body["postgres"].startswith("error")


async def test_ready_reports_degraded_when_rabbitmq_unavailable(client):
    _apply_overrides(rabbitmq_fails=True)

    response = await client.get("/api/v1/ready")

    body = response.json()
    assert response.status_code == 200
    assert body["status"] == "degraded"
    assert body["rabbitmq"].startswith("error")
