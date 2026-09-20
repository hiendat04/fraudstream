"""The inference API: fetch a payment's history, ask the model, answer."""

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError

from fraudstream_api.inference.features import history_found, model_inputs, usable_features
from fraudstream_api.inference.model import (
    ModelClient,
    ModelRejected,
    ModelTimeout,
    ModelUnavailable,
)
from fraudstream_api.inference.schemas import Prediction, Transaction
from fraudstream_api.inference.settings import Settings
from fraudstream_api.versioning import VersionHeader


def create_app(settings: Settings | None = None, *, reader=None, model=None) -> FastAPI:
    """Build the app. Tests pass their own reader and model; nothing else does."""

    config = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        from fraudstream_api.inference.online_store import FeastReader

        app.state.reader = reader or FeastReader(config)
        app.state.model = model or ModelClient(config.model_url, config.model_timeout_seconds)
        # Uvicorn opens its port only after this returns, so no request meets a cold store.
        await app.state.reader.start()
        yield
        await app.state.model.close()
        await app.state.reader.close()

    app = FastAPI(title="FraudStream inference API", lifespan=lifespan)
    app.add_middleware(VersionHeader, version=config.app_version)

    @app.post("/v1/predict", response_model=Prediction)
    async def predict(transaction: Transaction) -> Prediction:
        try:
            online = await app.state.reader.read(transaction.customer_id, transaction.merchant_id)
        except RedisError as error:
            raise HTTPException(503, "the online store cannot be reached") from error

        features = usable_features(online, transaction.event_timestamp, app.state.reader.ttls())
        inputs = model_inputs(transaction, features)

        try:
            probability = await app.state.model.score(inputs)
        except ModelTimeout as error:
            raise HTTPException(504, "the model did not answer in time") from error
        except ModelUnavailable as error:
            raise HTTPException(503, "the model cannot be reached") from error
        except ModelRejected as error:
            raise HTTPException(502, f"the model refused the request: {error}") from error

        return Prediction(
            transaction_id=transaction.transaction_id,
            fraud_probability=probability,
            history_found=history_found(inputs),
        )

    @app.get("/livez")
    async def livez() -> dict[str, str]:
        # Checks nothing else. Restarting this pod cannot fix Redis or the model.
        return {"status": "alive"}

    @app.get("/readyz")
    async def readyz():
        # Never calls the model. Probe traffic would stop it from ever scaling to zero.
        if await app.state.reader.is_ready():
            return {"status": "ready"}
        return JSONResponse({"status": "not ready", "reason": "the online store does not answer"}, 503)

    return app
