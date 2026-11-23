import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from nexus.api.security import Principal, authenticate, hash_api_key, require_cron_secret
from nexus.infrastructure.config import Settings


def _settings(**values: str) -> Settings:
    return Settings(_env_file=None, **values)  # type: ignore[call-arg]


def _bearer(token: str) -> HTTPAuthorizationCredentials:
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


def test_valid_key_resolves_its_tenant():
    settings = _settings(
        api_keys=f"acme:{hash_api_key('secret-a')},globex:{hash_api_key('secret-b')}"
    )
    assert authenticate(_bearer("secret-b"), settings) == Principal(tenant_id="globex")


@pytest.mark.parametrize("token", ["wrong", ""])
def test_unknown_key_is_rejected(token):
    settings = _settings(api_keys=f"acme:{hash_api_key('secret-a')}")
    with pytest.raises(HTTPException) as exc:
        authenticate(_bearer(token), settings)
    assert exc.value.status_code == 401


def test_missing_credentials_are_rejected():
    with pytest.raises(HTTPException) as exc:
        authenticate(None, _settings(api_keys=f"acme:{hash_api_key('x')}"))
    assert exc.value.status_code == 401


def test_malformed_api_keys_config_fails_loudly():
    with pytest.raises(ValueError):
        authenticate(_bearer("x"), _settings(api_keys="acme:not-a-digest"))


def test_cron_endpoint_hidden_when_secret_unset():
    with pytest.raises(HTTPException) as exc:
        require_cron_secret(_bearer("anything"), _settings())
    assert exc.value.status_code == 404


def test_cron_secret_must_match():
    settings = _settings(CRON_SECRET="s3cret")
    require_cron_secret(_bearer("s3cret"), settings)
    with pytest.raises(HTTPException) as exc:
        require_cron_secret(_bearer("nope"), settings)
    assert exc.value.status_code == 401
