"""Configuration for the context API, read from environment variables."""

from pydantic import SecretStr
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Credentials arrive from a Kubernetes Secret, never from the image."""

    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "fraudstream"
    postgres_user: str
    postgres_password: SecretStr
    redis_host: str = "redis"
    redis_port: int = 6379
    pgvector_host: str = "pgvector"
    pgvector_port: int = 5432
    pgvector_db: str = "llm"
    pgvector_user: str
    pgvector_password: SecretStr
    app_version: str = "dev"
    # Unset means no metrics endpoint: tests and one-off runs need none.
    metrics_port: int | None = None
    # Where to send traces, for example http://jaeger.observability:4318. Unset means no traces.
    otel_exporter_otlp_endpoint: str | None = None
