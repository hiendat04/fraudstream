"""Asks the agents their golden questions through port-forwards, and checks the tool calls and the answers.

    python tools/check_agent.py --controller http://127.0.0.1:18083 --context-api http://127.0.0.1:18093 \
        --inference-api http://127.0.0.1:18092 [--agent risk-agent]...
    python tools/check_agent.py --controller ... --context-api ... --inference-api ... --agent context-agent --load 3
    python tools/check_agent.py --controller ... --context-api ... --inference-api ... --agent context-agent --load 10 --concurrency 2

Every agent's questions are asked when no --agent is given. An answer passes when
the agent finished, made the tool calls the question needs, and its answer holds
what the stores and the fraud detection model say. --load sends one agent's first
question that many times at once, to watch KEDA add a replica, and --concurrency
sends them at most that many at a time. A sandboxed agent's question waits for a
free worker, the time it waits counted in its seconds. Exits 1 if any check fails.
"""

import argparse
import asyncio
import sys
import time
from uuid import uuid4

import httpx

from fraudstream_agents.a2a import A2AError, Answer, agent_url, ask
from fraudstream_agents.evals import GOLDEN_QUESTIONS, GoldenQuestion, problems

# The model has one slot, so questions sent together queue behind each other, and the
# coordinator's question waits for two specialists in turn.
ANSWER_TIMEOUT_SECONDS = 900
RETRY_SECONDS = 2


async def ask_when_free(client: httpx.AsyncClient, url: str, question: str) -> Answer:
    """Ask, and ask again while Substrate refuses a new session because every worker is busy, as its refusal says to.

    Every try is in the same session: a refused request still leaves its session behind.
    """

    started, session = time.perf_counter(), uuid4().hex
    while True:
        try:
            return await ask(client, url, question, session)
        except A2AError as error:
            if "no free workers" not in str(error) or time.perf_counter() - started >= ANSWER_TIMEOUT_SECONDS:
                raise
        await asyncio.sleep(RETRY_SECONDS)


async def ask_and_check(client: httpx.AsyncClient, args: argparse.Namespace, golden: GoldenQuestion, judge: bool
) -> bool:
    """Ask one golden question, print the line and any problems; whether it passed. Without judge, only the state counts."""

    started = time.perf_counter()
    try:
        answer = await ask_when_free(client, agent_url(args.controller, golden.agent), golden.question)
    except (A2AError, httpx.HTTPError) as error:
        answer = Answer("error", repr(error), [])
    seconds = time.perf_counter() - started
    found = await problems(client, golden, answer, args.context_api, args.inference_api) if judge else []
    if not judge and answer.state != "completed":
        found = [f"the answer ended {answer.state}"]
    print(f"  {'FAIL' if found else 'ok  '}  {seconds:5.1f} s  {answer.state}  [{', '.join(answer.tools)}]  {golden.question}")
    print("        " + answer.text[:300].replace("\n", " "))
    for problem in found:
        print(f"        - {problem}")
    return not found


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """The command line; --concurrency defaults to --load, and must lie between 1 and it."""

    parser = argparse.ArgumentParser()
    parser.add_argument("--controller", required=True, help="kagent's controller, which serves every agent's A2A endpoint")
    parser.add_argument("--context-api", required=True)
    parser.add_argument("--inference-api", required=True)
    parser.add_argument("--agent", action="append", choices=sorted({golden.agent for golden in GOLDEN_QUESTIONS}))
    parser.add_argument("--load", type=int, default=0, help="ask the agent's first question this many times at once")
    parser.add_argument("--concurrency", type=int, help="ask the --load questions at most this many at a time")
    args = parser.parse_args(argv)
    if args.load and len(args.agent or []) != 1:
        parser.error("--load needs exactly one --agent")
    if args.concurrency is None:
        args.concurrency = args.load
    elif not 1 <= args.concurrency <= args.load:
        parser.error("--concurrency must be between 1 and --load")
    return args


async def main() -> int:
    args = parse_args()
    chosen = [golden for golden in GOLDEN_QUESTIONS if not args.agent or golden.agent in args.agent]

    async with httpx.AsyncClient(timeout=ANSWER_TIMEOUT_SECONDS) as client:
        if args.load:
            gate = asyncio.Semaphore(args.concurrency)

            async def gated() -> bool:
                async with gate:
                    return await ask_and_check(client, args, chosen[0], judge=False)

            results = await asyncio.gather(*(gated() for _ in range(args.load)))
        else:
            results = [await ask_and_check(client, args, golden, judge=True) for golden in chosen]
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
