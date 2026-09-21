"""Sends each scored input row to the drift detection API.

Never raises. A prediction must not fail because the drift detector is down,
and the caller has already been answered by the time this runs.
"""

import logging

import httpx

log = logging.getLogger(__name__)


class DriftFeed:
    def __init__(
        self,
        url: str,
        timeout_seconds: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self._url = url
        self._http = httpx.AsyncClient(timeout=timeout_seconds, transport=transport)

    async def send(self, inputs: dict[str, float | bool | None]) -> None:
        try:
            response = await self._http.post(self._url, json={"observations": [inputs]})
        except httpx.HTTPError as error:
            log.warning("drift detector unreachable: %r", error)
            return
        if response.status_code != 200:
            log.warning(
                "drift detector refused an observation: %s %s",
                response.status_code,
                response.text[:200],
            )

    async def close(self) -> None:
        await self._http.aclose()
