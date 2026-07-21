from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "nexus"
    environment: str = "local"
    debug: bool = False

    postgres_user: str = "nexus"
    postgres_password: str = "nexus"
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "nexus"

    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0

    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "nexus"
    rabbitmq_password: str = "nexus"

    outbox_relay_poll_interval_seconds: float = 1.0
    outbox_relay_max_attempts: int = 8
    outbox_relay_initial_backoff_seconds: float = 5.0
    outbox_relay_max_backoff_seconds: float = 3600.0

    scheduler_lease_duration_seconds: float = 15.0

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    @property
    def rabbitmq_url(self) -> str:
        return (
            f"amqp://{self.rabbitmq_user}:{self.rabbitmq_password}"
            f"@{self.rabbitmq_host}:{self.rabbitmq_port}/"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
