"""Fixed-window rate limiting per principal (API key tenant) and scope.

Uses Redis when configured (shared across Function instances and API
replicas: INCR + EXPIRE on `rl:{scope}:{identity}:{window}`), otherwise an
in-process limiter -- adequate for a single local process, and documented as
per-instance on serverless. Exceeding the limit returns 429 with
Retry-After."""

import time
from collections import defaultdict
from collections.abc import Awaitable, Callable

from fastapi import Depends, HTTPException, Request, status
from redis.asyncio import Redis

from nexus.api.security import Principal, authenticate
from nexus.infrastructure.cache.redis import get_redis
from nexus.infrastructure.observability.metrics import rate_limited_requests_total

_WINDOW_SECONDS = 60


class InMemoryRateLimiter:
    def __init__(self) -> None:
        self._counts: dict[tuple[str, int], int] = defaultdict(int)

    def hit(self, key: str, window: int) -> int:
        if len(self._counts) > 10_000:
            self._counts = defaultdict(
                int, {k: v for k, v in self._counts.items() if k[1] >= window}
            )
        self._counts[(key, window)] += 1
        return self._counts[(key, window)]


_local = InMemoryRateLimiter()


def rate_limit(scope: str) -> Callable[..., Awaitable[Principal]]:
    async def dependency(
        request: Request,
        principal: Principal = Depends(authenticate),
        redis: Redis | None = Depends(get_redis),
    ) -> Principal:
        settings = request.app.state.container.settings
        limit = (
            settings.rate_limit_writes_per_minute
            if scope == "write"
            else settings.rate_limit_reads_per_minute
        )
        if limit <= 0:
            return principal
        now = time.time()
        window = int(now // _WINDOW_SECONDS)
        key = f"rl:{scope}:{principal.tenant_id}"
        count: int
        try:
            if redis is None:
                raise LookupError
            redis_key = f"{key}:{window}"
            count = int(await redis.incr(redis_key))
            if count == 1:
                await redis.expire(redis_key, _WINDOW_SECONDS + 5)
        except Exception:
            # Redis unavailable or not configured: degrade to per-process.
            count = _local.hit(key, window)
        if count > limit:
            rate_limited_requests_total.labels(scope=scope).inc()
            retry_after = int(_WINDOW_SECONDS - (now % _WINDOW_SECONDS)) + 1
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail={"code": "rate_limited", "message": f"limit is {limit} per minute"},
                headers={"Retry-After": str(retry_after)},
            )
        return principal

    return dependency
