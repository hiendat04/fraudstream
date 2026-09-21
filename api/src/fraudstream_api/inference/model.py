"""Asks the KServe fraud model for a score."""

import httpx


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

    async def score(self, inputs: dict[str, float | bool | None]) -> float:
        try:
            response = await self._http.post(self._url, json={"instances": [inputs]})
        # A timeout is also a transport error, so it is caught first.
        except httpx.TimeoutException as error:
            raise ModelTimeout(str(error)) from error
        except httpx.TransportError as error:
            raise ModelUnavailable(str(error)) from error
        if response.status_code != 200:
            raise ModelRejected(f"{response.status_code}: {response.text[:200]}")
        return float(response.json()["predictions"][0])

    async def close(self) -> None:
        await self._http.aclose()
