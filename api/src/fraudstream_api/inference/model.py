"""Asks the KServe fraud model for a score, and which model version gave it."""

from dataclasses import dataclass

import httpx

# A failed call has no answer, and an older model server sends no version.
UNKNOWN_VERSION = "unknown"


@dataclass(frozen=True)
class ModelAnswer:
    probability: float
    version: str
    threshold: float | None


class ModelTimeout(Exception):
    """The model did not answer in time."""


class ModelUnavailable(Exception):
    """The model could not be reached."""


class ModelRejected(Exception):
    """The model answered with an error."""


class ModelClient:
    """One connection pool, reused for every request."""

    def __init__(
        self,
        url: str,
        timeout_seconds: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self._url = url
        self._http = httpx.AsyncClient(timeout=timeout_seconds, transport=transport)

    async def score(self, inputs: dict[str, float | bool | None]) -> ModelAnswer:
        try:
            response = await self._http.post(self._url, json={"instances": [inputs]})
        # A timeout is also a transport error, so it is caught first.
        except httpx.TimeoutException as error:
            raise ModelTimeout(str(error)) from error
        except httpx.TransportError as error:
            raise ModelUnavailable(str(error)) from error
        if response.status_code != 200:
            raise ModelRejected(f"{response.status_code}: {response.text[:200]}")
        body = response.json()
        threshold = body.get("threshold")
        return ModelAnswer(
            probability=float(body["predictions"][0]),
            version=str(body.get("model_version") or UNKNOWN_VERSION),
            threshold=None if threshold is None else float(threshold),
        )

    async def close(self) -> None:
        await self._http.aclose()
