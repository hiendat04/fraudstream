import asyncio
import time
from collections.abc import Awaitable, Callable

import httpx

from benchmark.metrics import RequestResult


async def send_streaming_request(
    client: httpx.AsyncClient, base_url: str, prompt: str
) -> RequestResult:
    payload = {
        "model": "default_model",
        "messages": [{"role": "user", "content": prompt}],
        "stream": True,
        "max_tokens": 200,
    }
    start = time.monotonic()
    first_token_time: float | None = None
    output_tokens = 0

    try:
        async with client.stream(
            "POST", f"{base_url}/v1/chat/completions", json=payload, timeout=60.0
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line.startswith("data:"):
                    continue
                data = line[len("data:"):].strip()
                if data == "[DONE]":
                    break
                if first_token_time is None:
                    first_token_time = time.monotonic()
                output_tokens += 1
    except (httpx.HTTPError, httpx.TimeoutException):
        return RequestResult(
            ttft_seconds=0.0, total_seconds=0.0, output_tokens=0, succeeded=False
        )

    end = time.monotonic()
    if first_token_time is None:
        return RequestResult(
            ttft_seconds=0.0, total_seconds=0.0, output_tokens=0, succeeded=False
        )

    return RequestResult(
        ttft_seconds=first_token_time - start,
        total_seconds=end - start,
        output_tokens=output_tokens,
        succeeded=True,
    )


async def run_concurrency_level(
    base_url: str,
    prompts: list[str],
    concurrency: int,
    request_fn: Callable[
        [httpx.AsyncClient, str, str], Awaitable[RequestResult]
    ] = send_streaming_request,
) -> tuple[list[RequestResult], float]:
    semaphore = asyncio.Semaphore(concurrency)

    async def bounded_request(client: httpx.AsyncClient, prompt: str) -> RequestResult:
        async with semaphore:
            return await request_fn(client, base_url, prompt)

    start = time.monotonic()
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*(bounded_request(client, p) for p in prompts))
    wall_clock = time.monotonic() - start

    return list(results), wall_clock
