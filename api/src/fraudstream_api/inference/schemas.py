"""The payment the pull API scores, and its answer."""

from datetime import UTC, datetime, timedelta

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from fraudstream_ml.features import CHANNELS, CITIES

# A clock a little ahead of the server is drift, not a payment from the future.
CLOCK_SKEW = timedelta(minutes=5)


class Transaction(BaseModel):
    """One payment. The two IDs find its history; the rest describes the payment."""

    model_config = ConfigDict(extra="forbid")

    transaction_id: str = Field(min_length=1, max_length=64)
    customer_id: str = Field(min_length=1, max_length=64)
    merchant_id: str = Field(min_length=1, max_length=64)
    event_timestamp: AwareDatetime
    amount: float = Field(gt=0)
    channel: str
    city: str

    @field_validator("event_timestamp")
    @classmethod
    def in_utc_and_not_in_the_future(cls, value: datetime) -> datetime:
        value = value.astimezone(UTC)
        if value > datetime.now(UTC) + CLOCK_SKEW:
            raise ValueError("event_timestamp is in the future")
        return value

    @field_validator("channel")
    @classmethod
    def known_channel(cls, value: str) -> str:
        if value not in CHANNELS:
            raise ValueError(f"channel must be one of: {', '.join(CHANNELS)}")
        return value

    @field_validator("city")
    @classmethod
    def known_city(cls, value: str) -> str:
        if value not in CITIES:
            raise ValueError(f"city must be one of: {', '.join(CITIES)}")
        return value


class Prediction(BaseModel):
    transaction_id: str
    fraud_probability: float
    history_found: dict[str, bool]
