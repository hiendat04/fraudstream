"""Configuration, read from environment variables."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database 1: the drift window never shares a keyspace with the feature store.
    redis_url: str = "redis://redis:6379/1"
    reference_path: str = "/app/reference/fraud-detection-v2.json"
    window_minutes: int = 60
    # Below this, a PSI says more about chance than about drift.
    min_observations: int = 1000
    app_version: str = "dev"
