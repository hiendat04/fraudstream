"""Asks the context agent about a customer and a merchant, through port-forwards, and wants numbers from the store.

    python tools/check_agent.py --controller http://127.0.0.1:18083/api/a2a/kagent/context-agent/ --context-api http://127.0.0.1:18093
    python tools/check_agent.py --controller ... --context-api ... --load 6

Each answer passes when the agent finished and quotes at least one of the values
the context API holds for that id. --load sends the customer question that many
times at once, to watch KEDA add a replica. Exits 1 if any check fails.
"""

import argparse
import asyncio
import sys
import time

import httpx

from fraudstream_agents.a2a import A2AError, ask, grounded

QUESTIONS = [
    ("What has customer cust_00090571 been doing recently?", "/v1/customers/cust_00090571"),
    ("How risky is merchant merch_00011503 right now?", "/v1/merchants/merch_00011503"),
]
# The model has one slot, so questions sent together queue behind each other.
ANSWER_TIMEOUT_SECONDS = 600


def stored_values(body: dict) -> list[float]:
    """Every numeric feature value the context API returned, in all views."""

    return [
        value
        for view in body["views"].values()
        for value in view["features"].values()
        if isinstance(value, int | float) and not isinstance(value, bool)
    ]


async def one(client: httpx.AsyncClient, controller: str, question: str, values: list[float] | None) -> bool:
    started = time.perf_counter()
    try:
        state, text = await ask(client, controller, question)
    except (A2AError, httpx.HTTPError) as error:
        state, text = "error", repr(error)
    seconds = time.perf_counter() - started
    ok = state == "completed" and (values is None or grounded(text, values))
    print(f"  {'ok  ' if ok else 'FAIL'}  {seconds:5.1f} s  {state}  {question}")
    print("        " + text[:300].replace("\n", " "))
    return ok


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--controller", required=True, help="the agent's A2A URL on kagent's controller")
    parser.add_argument("--context-api", required=True)
    parser.add_argument("--load", type=int, default=0, help="ask the customer question this many times at once")
    args = parser.parse_args()

    async with httpx.AsyncClient(timeout=ANSWER_TIMEOUT_SECONDS) as client:
        if args.load:
            results = await asyncio.gather(*(one(client, args.controller, QUESTIONS[0][0], None) for _ in range(args.load)))
            return 0 if all(results) else 1
        results = []
        for question, path in QUESTIONS:
            truth = await client.get(args.context_api + path)
            truth.raise_for_status()
            results.append(await one(client, args.controller, question, stored_values(truth.json())))
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
