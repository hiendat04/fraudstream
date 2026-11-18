"""What the context API answers: an entity's features, a chunk, and a search."""

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

# Customer and merchant ids look like cust_00090571. Nothing else may reach the URL.
ENTITY_ID_PATTERN = r"^[A-Za-z0-9_-]{1,64}$"
MAX_TOP_K = 10
MAX_QUESTION_CHARS = 500


class View(BaseModel):
    """One feature view: when the store last received it, and its values. None means nothing stored."""

    as_of: datetime | None
    features: dict[str, float | int | str | None]


class EntityFeatures(BaseModel):
    entity_id: str
    views: dict[str, View]


class CustomerOnDevice(BaseModel):
    customer_id: str
    payments: int
    fraud: bool


class DeviceNeighbours(BaseModel):
    """Who paid on one device in the 30 days up to its newest payment: the counts, and at most 10 customers, fraud first."""

    device_id: str
    window_end: datetime
    customer_count: int
    fraud_customer_count: int
    ip_customer_count: int
    customers: list[CustomerOnDevice]


class Chunk(BaseModel):
    chunk_id: str
    doc_id: str
    title: str
    heading: str
    text: str


class SearchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=MAX_QUESTION_CHARS)
    ]
    top_k: int = Field(default=5, ge=1, le=MAX_TOP_K)


class Hit(Chunk):
    distance: float


class SearchAnswer(BaseModel):
    hits: list[Hit]
