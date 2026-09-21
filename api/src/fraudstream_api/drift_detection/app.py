"""The drift detection API: count what the model scores, and say how far it has moved."""

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from redis.asyncio import Redis
from redis.exceptions import RedisError

from fraudstream_api.drift_detection.reference import Reference, compare
from fraudstream_api.drift_detection.settings import Settings
from fraudstream_api.drift_detection.window import DriftWindow
from fraudstream_api.versioning import VersionHeader

SEVERITY = {"stable": 0, "warning": 1, "drift": 2}
MAX_BATCH = 5000


class Observations(BaseModel):
    """One batch of the model's input rows, as the model received them."""

    model_config = ConfigDict(extra="forbid")

    observations: list[dict[str, float | bool | None]] = Field(min_length=1, max_length=MAX_BATCH)


class FeatureDriftOut(BaseModel):
    name: str
    psi: float
    status: Literal["stable", "warning", "drift"]


class DriftReport(BaseModel):
    model_name: str
    model_version: str
    window_minutes: int
    observations: int
    status: Literal["stable", "warning", "drift", "not_enough_data"]
    features: list[FeatureDriftOut]


def create_app(settings: Settings | None = None, *, redis: Redis | None = None) -> FastAPI:
    """Build the app. Tests pass their own Redis; nothing else does."""

    config = settings or Settings()
    reference = Reference.from_json(Path(config.reference_path).read_text())
    client = redis or Redis.from_url(
        config.redis_url, decode_responses=True, socket_timeout=1, socket_connect_timeout=1
    )
    window = DriftWindow(client, reference, minutes=config.window_minutes)
    expected = set(reference.features)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Fail the start-up, not the first request, when Redis is unreachable.
        await client.ping()
        yield
        await client.aclose()

    app = FastAPI(title="FraudStream drift detection API", lifespan=lifespan)
    app.add_middleware(VersionHeader, version=config.app_version)

    @app.post("/v1/observations")
    async def observe(body: Observations) -> dict[str, int]:
        for position, row in enumerate(body.observations):
            missing = expected - row.keys()
            unknown = row.keys() - expected
            if missing or unknown:
                raise HTTPException(
                    422,
                    {
                        "observation": position,
                        "missing": sorted(missing),
                        "unknown": sorted(unknown),
                    },
                )
        return {"accepted": await window.add(body.observations)}

    @app.get("/v1/drift", response_model=DriftReport)
    async def drift() -> DriftReport:
        rows, counts = await window.totals()
        described = {
            "model_name": reference.model_name,
            "model_version": reference.model_version,
            "window_minutes": config.window_minutes,
            "observations": rows,
        }
        if rows < config.min_observations:
            return DriftReport(**described, status="not_enough_data", features=[])

        drifts = compare(reference, counts, rows)
        worst = max(drifts, key=lambda one: SEVERITY[one.status]).status
        return DriftReport(
            **described,
            status=worst,
            features=[
                FeatureDriftOut(name=one.name, psi=one.psi, status=one.status) for one in drifts
            ],
        )

    @app.get("/livez")
    async def livez() -> dict[str, str]:
        # Checks nothing else. Restarting this pod cannot fix Redis.
        return {"status": "alive"}

    @app.get("/readyz")
    async def readyz():
        try:
            await client.ping()
            return {"status": "ready"}
        except RedisError:
            return JSONResponse({"status": "not ready", "reason": "Redis does not answer"}, 503)

    return app
