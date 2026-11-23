import pytest
from pydantic import ValidationError

from nexus.infrastructure.config import (
    EventBus,
    Settings,
    normalize_async_database_url,
)

_KEY = "acme:" + "a" * 64


def _settings(**env: str) -> Settings:
    return Settings(_env_file=None, **env)  # type: ignore[call-arg]


def test_neon_style_url_is_normalized_for_asyncpg():
    url = normalize_async_database_url(
        "postgres://u:p%40ss@ep-x-pooler.us-east-1.aws.neon.tech/neondb"
        "?sslmode=require&channel_binding=require&application_name=x"
    )
    assert url.startswith("postgresql+asyncpg://u:p%40ss@ep-x-pooler")
    assert "sslmode" not in url
    assert "channel_binding" not in url
    assert "application_name=x" in url


def test_database_url_env_wins_and_detects_tls_and_pooler(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@ep-x-pooler.neon.tech/db?sslmode=require")
    settings = _settings()
    assert settings.database_url == "postgresql+asyncpg://u:p@ep-x-pooler.neon.tech/db"
    assert settings.database_requires_ssl is True
    assert settings.database_behind_pgbouncer is True


def test_migrations_prefer_unpooled_url(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@ep-x-pooler.neon.tech/db")
    monkeypatch.setenv("DATABASE_URL_UNPOOLED", "postgresql://u:p@ep-x.neon.tech/db")
    settings = _settings()
    assert "pooler" not in settings.migration_database_url
    assert "pooler" in settings.database_url


def test_discrete_fields_are_url_encoded():
    settings = _settings(postgres_password="p@ss/word:1")
    assert "p%40ss%2Fword%3A1@" in settings.database_url


def test_production_rejects_default_broker_credentials(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@db.example.com/db")
    with pytest.raises(ValidationError, match="RabbitMQ credentials"):
        _settings(environment="production", api_keys=_KEY)


def test_rabbitmq_url_override_wins(monkeypatch):
    monkeypatch.setenv("RABBITMQ_URL", "amqps://u:p@broker.example.com/vhost")
    assert _settings().rabbitmq_url == "amqps://u:p@broker.example.com/vhost"


def test_production_rejects_default_credentials_and_missing_api_keys():
    with pytest.raises(ValidationError, match="built-in defaults"):
        _settings(environment="production", api_keys=_KEY)
    with pytest.raises(ValidationError, match="API_KEYS is empty"):
        _settings(environment="production", postgres_password="strong")


def test_production_accepts_explicit_configuration(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@db.example.com/db")
    monkeypatch.setenv("RABBITMQ_URL", "amqps://u:p@broker.example.com/vhost")
    settings = _settings(environment="production", api_keys=_KEY)
    assert settings.environment == "production"


def test_vercel_env_selects_environment_and_keeps_rabbitmq(monkeypatch):
    # RabbitMQ stays the bus on Vercel unless Vercel Queues is chosen explicitly.
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@db.example.com/db")
    monkeypatch.setenv("RABBITMQ_PASSWORD", "strong")
    settings = _settings(api_keys=_KEY)
    assert settings.environment == "preview"
    assert settings.event_bus is EventBus.RABBITMQ


def test_explicit_event_bus_is_respected_on_vercel(monkeypatch):
    monkeypatch.setenv("VERCEL_ENV", "preview")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@db.example.com/db")
    settings = _settings(api_keys=_KEY, event_bus="none")
    assert settings.event_bus is EventBus.NONE


def test_redis_is_optional():
    assert _settings().redis_url is None
    assert _settings(redis_enabled="true").redis_url == "redis://localhost:6379/0"


def test_daemon_only_publishes_when_it_can_name_the_push_partition(monkeypatch):
    from nexus.daemon import can_publish_to_push_consumers

    monkeypatch.setenv("VERCEL_ENV", "production")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@db.example.com/db")
    monkeypatch.setenv("EVENT_BUS", "vercel_queues")
    assert can_publish_to_push_consumers(_settings(api_keys=_KEY)) is False

    monkeypatch.setenv("VERCEL_DEPLOYMENT_ID", "dpl_123")
    assert can_publish_to_push_consumers(_settings(api_keys=_KEY)) is True

    monkeypatch.delenv("VERCEL_DEPLOYMENT_ID")
    explicit = _settings(api_keys=_KEY, vercel_queue_deployment="all")
    assert can_publish_to_push_consumers(explicit) is True

    monkeypatch.setenv("EVENT_BUS", "rabbitmq")
    monkeypatch.setenv("RABBITMQ_URL", "amqps://u:p@broker.example.com/vhost")
    assert can_publish_to_push_consumers(_settings(api_keys=_KEY)) is True


def test_production_rejects_published_development_secrets(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@db.example.com/db")
    monkeypatch.setenv("RABBITMQ_URL", "amqps://u:p@broker.example.com/vhost")
    dev_key = "dev:7e9f8fd111802be56c379d597842e29b2cebd35ff2133d431a49fa556a18704e"
    with pytest.raises(ValidationError, match="public development key"):
        _settings(environment="production", api_keys=f"{_KEY},{dev_key}")
    with pytest.raises(ValidationError, match="CRON_SECRET is the public local default"):
        _settings(environment="production", api_keys=_KEY, CRON_SECRET="local-cron-secret")
    with pytest.raises(ValidationError, match="METRICS_TOKEN is the public local default"):
        _settings(environment="production", api_keys=_KEY, metrics_token="local-metrics-token")
    # The same values stay valid for local development.
    assert _settings(api_keys=dev_key, CRON_SECRET="local-cron-secret").is_local
