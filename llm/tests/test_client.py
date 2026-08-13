import asyncio

from benchmark.client import run_concurrency_level
from benchmark.metrics import RequestResult


async def fake_request_fn(client, base_url, prompt):
    await asyncio.sleep(0.01)
    return RequestResult(
        ttft_seconds=0.01, total_seconds=0.02, output_tokens=10, succeeded=True
    )


def test_run_concurrency_level_returns_one_result_per_prompt():
    prompts = ["a", "b", "c", "d"]

    results, wall_clock = asyncio.run(
        run_concurrency_level(
            "http://localhost:8080", prompts, concurrency=2, request_fn=fake_request_fn
        )
    )

    assert len(results) == 4
    assert all(r.succeeded for r in results)
    assert wall_clock > 0


def test_run_concurrency_level_respects_concurrency_limit():
    active = 0
    max_active = 0
    lock = asyncio.Lock()

    async def tracking_request_fn(client, base_url, prompt):
        nonlocal active, max_active
        async with lock:
            active += 1
            max_active = max(max_active, active)
        await asyncio.sleep(0.02)
        async with lock:
            active -= 1
        return RequestResult(
            ttft_seconds=0.01, total_seconds=0.02, output_tokens=5, succeeded=True
        )

    prompts = ["p"] * 10

    asyncio.run(
        run_concurrency_level(
            "http://localhost:8080", prompts, concurrency=3, request_fn=tracking_request_fn
        )
    )

    assert max_active <= 3
