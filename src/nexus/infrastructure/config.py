from enum import StrEnum
from functools import lru_cache
from typing import Self
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL


class EventBus(StrEnum):
    """Where the outbox relay publishes events.

    vercel_queues: Vercel Queues (the deployment target; also used by tests
        through the SDK's embedded queue service).
    rabbitmq: the original AMQP adapter, kept for local/self-hosted runs.
    none: events stay in the outbox (useful for API-only test setups).
    """

    VERCEL_QUEUES = "vercel_queues"
    RABBITMQ = "rabbitmq"
    NONE = "none"


class DbPoolMode(StrEnum):
    """queue: a small SQLAlchemy QueuePool per process. Correct for Vercel
    Fluid compute (one instance serves many concurrent invocations and is kept
    alive long enough to close idle connections), for daemons and locally.
    null: open/close a connection per checkout (NullPool). Use when an
    external pooler such as Neon's PgBouncer endpoint does all pooling.
    """

    QUEUE = "queue"
    NULL = "null"


_DEFAULT_PASSWORD = "nexus"  # noqa: S105 - local-only default, rejected outside local/test
_LOCAL_ENVIRONMENTS = frozenset({"local", "test", "development"})
# libpq query parameters asyncpg does not understand. sslmode is translated
# into asyncpg's `ssl` connect argument instead of being passed through.
_LIBPQ_ONLY_PARAMS = frozenset(
    {"sslmode", "channel_binding", "pgbouncer", "connect_timeout", "options"}
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "nexus"
    # Falls back to VERCEL_ENV (production/preview/development) when unset.
    environment: str = Field(default="local")
    debug: bool = False

    # --- PostgreSQL -----------------------------------------------------
    # A managed provider's connection string (Neon/Supabase/Prisma via the
    # Vercel Marketplace inject DATABASE_URL or POSTGRES_URL). When set it
    # wins over the discrete POSTGRES_* fields below.
    database_url_override: SecretStr | None = Field(default=None, alias="DATABASE_URL")
    postgres_url: SecretStr | None = Field(default=None, alias="POSTGRES_URL")
    # Direct (non-pooled) URL for migrations, where a provider offers one.
    database_url_unpooled: SecretStr | None = Field(default=None, alias="DATABASE_URL_UNPOOLED")
    postgres_url_non_pooling: SecretStr | None = Field(
        default=None, alias="POSTGRES_URL_NON_POOLING"
    )

    postgres_user: str = "nexus"
    postgres_password: SecretStr = SecretStr(_DEFAULT_PASSWORD)
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "nexus"

    db_pool_mode: DbPoolMode = DbPoolMode.QUEUE
    db_pool_size: int = 5
    db_max_overflow: int = 5
    db_pool_timeout_seconds: float = 10.0
    db_pool_recycle_seconds: int = 300
    db_statement_timeout_ms: int = 15_000
    # Set when DATABASE_URL points at a transaction-mode PgBouncer (e.g. a
    # Neon "-pooler" host): disables asyncpg's named prepared statement cache.
    db_behind_pgbouncer: bool | None = None

    # --- Redis (optional) -------------------------------------------------
    redis_url_override: SecretStr | None = Field(default=None, alias="REDIS_URL")
    redis_enabled: bool = False
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0

    # --- Messaging --------------------------------------------------------
    event_bus: EventBus = EventBus.RABBITMQ
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "nexus"
    rabbitmq_password: SecretStr = SecretStr(_DEFAULT_PASSWORD)
    # A managed broker URL (e.g. CloudAMQP: amqps://user:pass@host/vhost)
    # wins over the discrete fields.
    rabbitmq_url_override: SecretStr | None = Field(default=None, alias="RABBITMQ_URL")
    rabbitmq_publish_timeout_seconds: float = 10.0
    # Vercel Queues: messages can only be received from the region they were
    # sent to, so pin one region for send and receive (defaults to
    # VERCEL_REGION inside the SDK when unset).
    vercel_queue_region: str | None = None
    vercel_queue_dispatch_topic: str = "nexus-task-dispatch"
    vercel_queue_job_topic: str = "nexus-job-dispatch"
    vercel_queue_events_topic: str = "nexus-events"
    vercel_queue_publish_timeout_seconds: float = 10.0
    # Vercel Queues partitions topics by deployment. "auto": the current
    # deployment (VERCEL_DEPLOYMENT_ID, set in every Vercel Function) when
    # present, otherwise unpartitioned. "all": unpartitioned. Anything else
    # is taken as an explicit deployment ID. Push consumers only receive
    # messages for their own deployment, so producers that are not Functions
    # (daemons, local tools) must resolve to the same partition.
    vercel_queue_deployment: str = "auto"
    vercel_deployment_id: str | None = Field(default=None, alias="VERCEL_DEPLOYMENT_ID")

    # --- Outbox relay -----------------------------------------------------
    outbox_relay_poll_interval_seconds: float = 1.0
    outbox_relay_max_attempts: int = 8
    outbox_relay_initial_backoff_seconds: float = 5.0
    outbox_relay_max_backoff_seconds: float = 3600.0
    # Events a request relays itself right after committing (eager relay).
    outbox_eager_relay_max_events: int = 5

    # --- Scheduler / reconciler --------------------------------------------
    scheduler_lease_duration_seconds: float = 15.0
    reconciler_batch_size: int = 100
    # A READY task whose dispatch message has not been consumed for this long
    # is re-dispatched (the claim guard makes duplicates harmless).
    reconciler_redispatch_after_seconds: float = 300.0
    daemon_tick_interval_seconds: float = 5.0
    # The leader scheduler removes nexus.dead-letter messages whose job has
    # since been retried (see messaging/dead_letter.py). Up to N per pass.
    dead_letter_prune_resolved: bool = True
    dead_letter_prune_batch: int = Field(default=200, ge=1, le=5000)
    # Jobs
    worker_heartbeat_ttl_seconds: float = 30.0
    worker_heartbeat_interval_seconds: float = 5.0
    worker_forget_after_ttls: int = 20
    worker_capacity: int = 4
    worker_shutdown_grace_seconds: float = 25.0
    job_redispatch_after_seconds: float = 900.0
    job_default_timeout_seconds: int = 60
    job_max_timeout_seconds: int = 3600
    job_default_max_attempts: int = 3
    daemon_health_port: int = 8081

    # --- Task execution -----------------------------------------------------
    task_default_timeout_seconds: int = 60
    # Upper bound accepted at submission. Must stay below the queue consumer
    # function's maxDuration (vercel.json), minus lease grace.
    task_max_timeout_seconds: int = 240
    task_lease_grace_seconds: int = 30
    task_max_payload_bytes: int = 64 * 1024
    task_enable_test_handlers: bool = False
    webhook_allowed_hosts: str = ""

    # --- Security ---------------------------------------------------------
    # Comma-separated "tenant_id:sha256_hex_of_secret" entries. Generate with
    # `python -m nexus.cli hash-api-key <secret>`.
    api_keys: SecretStr = SecretStr("")
    # Vercel Cron sends `Authorization: Bearer $CRON_SECRET`.
    cron_secret: SecretStr | None = Field(default=None, alias="CRON_SECRET")
    metrics_token: SecretStr | None = None
    # Browser origins allowed to call the API (the dashboard), comma-separated.
    # Empty = same-origin only. "*" is rejected outside local/test.
    cors_allowed_origins: str = ""
    rate_limit_reads_per_minute: int = 600
    rate_limit_writes_per_minute: int = 120
    vercel_env: str | None = Field(default=None, alias="VERCEL_ENV")

    @model_validator(mode="after")
    def _resolve_environment(self) -> Self:
        if self.environment == "local" and self.vercel_env:
            self.environment = self.vercel_env
        return self

    @model_validator(mode="after")
    def _reject_insecure_defaults_outside_local(self) -> Self:
        if self.is_local:
            return self
        problems = []
        if self._primary_database_secret() is None and (
            self.postgres_password.get_secret_value() == _DEFAULT_PASSWORD
        ):
            problems.append("database credentials are the built-in defaults")
        if not self.api_keys.get_secret_value():
            problems.append("API_KEYS is empty")
        if (
            self.event_bus is EventBus.RABBITMQ
            and self.rabbitmq_url_override is None
            and self.rabbitmq_password.get_secret_value() == _DEFAULT_PASSWORD
        ):
            problems.append("RabbitMQ credentials are the built-in defaults (set RABBITMQ_URL)")
        if self.debug:
            problems.append("DEBUG must be false")
        if "*" in self.cors_origin_list:
            problems.append("CORS_ALLOWED_ORIGINS must list explicit origins, not '*'")
        for origin in self.cors_origin_list:
            if not origin.startswith("https://"):
                problems.append(f"CORS origin '{origin}' must use https")
        if problems:
            raise ValueError(
                f"Refusing to start in environment '{self.environment}': " + "; ".join(problems)
            )
        return self

    @property
    def is_local(self) -> bool:
        return self.environment in _LOCAL_ENVIRONMENTS

    def _primary_database_secret(self) -> SecretStr | None:
        return self.database_url_override or self.postgres_url

    def _unpooled_database_secret(self) -> SecretStr | None:
        return self.database_url_unpooled or self.postgres_url_non_pooling

    @property
    def database_url(self) -> str:
        """Async SQLAlchemy URL (postgresql+asyncpg) for the application."""
        secret = self._primary_database_secret()
        if secret is not None:
            return normalize_async_database_url(secret.get_secret_value())
        return URL.create(
            "postgresql+asyncpg",
            username=self.postgres_user,
            password=self.postgres_password.get_secret_value(),
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
        ).render_as_string(hide_password=False)

    @property
    def migration_database_url(self) -> str:
        """Migrations need a session-level connection: prefer the direct URL."""
        secret = self._unpooled_database_secret()
        if secret is not None:
            return normalize_async_database_url(secret.get_secret_value())
        return self.database_url

    @property
    def database_requires_ssl(self) -> bool:
        secret = self._primary_database_secret()
        if secret is None:
            return False
        query = dict(parse_qsl(urlsplit(secret.get_secret_value()).query))
        return query.get("sslmode", "") in {"require", "verify-ca", "verify-full"} or (
            query.get("ssl", "") in {"true", "require"}
        )

    @property
    def database_behind_pgbouncer(self) -> bool:
        if self.db_behind_pgbouncer is not None:
            return self.db_behind_pgbouncer
        secret = self._primary_database_secret()
        if secret is None:
            return False
        parts = urlsplit(secret.get_secret_value())
        return "-pooler." in (parts.hostname or "") or "pgbouncer=true" in parts.query

    @property
    def redis_url(self) -> str | None:
        if self.redis_url_override is not None:
            return self.redis_url_override.get_secret_value()
        if not self.redis_enabled:
            return None
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    @property
    def rabbitmq_url(self) -> str:
        if self.rabbitmq_url_override is not None:
            return self.rabbitmq_url_override.get_secret_value()
        return URL.create(
            "amqp",
            username=self.rabbitmq_user,
            password=self.rabbitmq_password.get_secret_value(),
            host=self.rabbitmq_host,
            port=self.rabbitmq_port,
            database="",
        ).render_as_string(hide_password=False)

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip().rstrip("/") for o in self.cors_allowed_origins.split(",") if o.strip()]

    @property
    def webhook_allowed_host_set(self) -> frozenset[str]:
        return frozenset(
            h.strip().lower() for h in self.webhook_allowed_hosts.split(",") if h.strip()
        )


def normalize_async_database_url(raw_url: str) -> str:
    """Turn a provider connection string (postgres://..., ?sslmode=require)
    into a URL SQLAlchemy's asyncpg dialect accepts. asyncpg rejects libpq-only
    query parameters, so they are stripped here; TLS is re-applied through
    connect_args (see infrastructure.db.session.engine_options)."""
    parts = urlsplit(raw_url)
    scheme = parts.scheme
    if scheme in {"postgres", "postgresql", "postgresql+psycopg", "postgresql+psycopg2"}:
        scheme = "postgresql+asyncpg"
    query = [(k, v) for k, v in parse_qsl(parts.query) if k not in _LIBPQ_ONLY_PARAMS]
    return urlunsplit((scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


@lru_cache
def get_settings() -> Settings:
    return Settings()
