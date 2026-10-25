"""Asks the agents their golden questions through port-forwards, and checks the tool calls and the answers.

    python tools/check_agent.py --controller http://127.0.0.1:18083 --context-api http://127.0.0.1:18093 \
        --inference-api http://127.0.0.1:18092 [--agent risk-agent]...
    python tools/check_agent.py --controller ... --context-api ... --inference-api ... --agent context-agent --load 3

Every agent's questions are asked when no --agent is given. An answer passes when
the agent finished, made the tool calls the question needs, and its answer holds
what the stores and the fraud detection model say. --load sends one agent's first
question that many times at once, to watch KEDA add a replica. Exits 1 if any check fails.
"""

import argparse
import asyncio
import sys
import time

import httpx

from fraudstream_agents.a2a import A2AError, Answer, ask
from fraudstream_agents.evals import GOLDEN_QUESTIONS, GoldenQuestion, problems

# The model has one slot, so questions sent together queue behind each other, and the
# coordinator's question waits for two specialists in turn.
ANSWER_TIMEOUT_SECONDS = 900


async def ask_and_check(client: httpx.AsyncClient, args: argparse.Namespace, golden: GoldenQuestion, judge: bool
) -> bool:
    """Ask one golden question, print the line and any problems; whether it passed. Without judge, only the state counts."""

    started = time.perf_counter()
    try:
        answer = await ask(client, f"{args.controller}/api/a2a/kagent/{golden.agent}/", golden.question)
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


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--controller", required=True, help="kagent's controller, which serves every agent's A2A endpoint")
    parser.add_argument("--context-api", required=True)
    parser.add_argument("--inference-api", required=True)
    parser.add_argument("--agent", action="append", choices=sorted({golden.agent for golden in GOLDEN_QUESTIONS}))
    parser.add_argument("--load", type=int, default=0, help="ask the agent's first question this many times at once")
    args = parser.parse_args()
    if args.load and len(args.agent or []) != 1:
        parser.error("--load needs exactly one --agent")
    chosen = [golden for golden in GOLDEN_QUESTIONS if not args.agent or golden.agent in args.agent]

    async with httpx.AsyncClient(timeout=ANSWER_TIMEOUT_SECONDS) as client:
        if args.load:
            results = await asyncio.gather(*(ask_and_check(client, args, chosen[0], judge=False) for _ in range(args.load)))
        else:
            results = [await ask_and_check(client, args, golden, judge=True) for golden in chosen]
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
