"""API authentication.

Clients authenticate with `Authorization: Bearer <api key>`. The server only
stores SHA-256 digests of keys (API_KEYS="tenant:sha256hex,..."), kept as a
Sensitive environment variable on Vercel. Each key is bound to one tenant;
every task query and mutation is scoped to that tenant, so tenants cannot
read or cancel each other's tasks.

Cron and metrics endpoints use separate shared secrets (CRON_SECRET, which
Vercel Cron sends automatically, and METRICS_TOKEN).
"""

import hashlib
import hmac
from dataclasses import dataclass
from functools import lru_cache

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from nexus.infrastructure.config import Settings

_bearer = HTTPBearer(auto_error=False)


@dataclass(frozen=True, slots=True)
class Principal:
    tenant_id: str


def hash_api_key(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


@lru_cache(maxsize=8)
def _parse_api_keys(raw: str) -> tuple[tuple[str, str], ...]:
    entries = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        tenant, sep, digest = item.rpartition(":")
        if not sep or not tenant or len(digest) != 64:
            raise ValueError("API_KEYS entries must look like 'tenant_id:<64-hex sha256>'")
        entries.append((tenant, digest.lower()))
    return tuple(entries)


def settings_from_request(request: Request) -> Settings:
    settings: Settings = request.app.state.container.settings
    return settings


def _unauthorized() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="invalid or missing credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )


def authenticate(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
    settings: Settings = Depends(settings_from_request),
) -> Principal:
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise _unauthorized()
    presented = hash_api_key(credentials.credentials)
    tenant: str | None = None
    for candidate_tenant, digest in _parse_api_keys(settings.api_keys.get_secret_value()):
        # Compare every entry (no early exit) in constant time per entry.
        if hmac.compare_digest(presented, digest):
            tenant = candidate_tenant
    if tenant is None:
        raise _unauthorized()
    return Principal(tenant_id=tenant)


def _require_secret(credentials: HTTPAuthorizationCredentials | None, secret: str | None) -> None:
    if not secret:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    if credentials is None or not hmac.compare_digest(credentials.credentials, secret):
        raise _unauthorized()


def require_cron_secret(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
    settings: Settings = Depends(settings_from_request),
) -> None:
    _require_secret(
        credentials, settings.cron_secret.get_secret_value() if settings.cron_secret else None
    )


def require_metrics_token(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
    settings: Settings = Depends(settings_from_request),
) -> None:
    _require_secret(
        credentials,
        settings.metrics_token.get_secret_value() if settings.metrics_token else None,
    )
