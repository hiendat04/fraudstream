"""Configuration, read from environment variables."""

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
    model_url: str = (
        "http://fraud-detection-predictor.kserve-models.svc.cluster.local"
        "/v1/models/fraud-detection:predict"
    )
    # The model scales to zero. Its first answer after a quiet spell took 5.9 s.
    model_timeout_seconds: float = 30.0
    # Unset means the inference API runs on its own.
    drift_url: str | None = None
    drift_timeout_seconds: float = 2.0
    app_version: str = "dev"
