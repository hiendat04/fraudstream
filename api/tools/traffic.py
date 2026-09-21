"""Send one request at a steady rate and count the answers, second by second.

Each line shows the answers that came back in that second, grouped by outcome
and by the app version that gave them. Exits 1 if anything failed.

    cd api && uv run python tools/traffic.py --host inference.localhost \
        --body tools/transaction.json --rate 10 --seconds 150
"""

import argparse
import asyncio
import collections
import json
import sys
import time
from pathlib import Path

import httpx


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--url", default="http://localhost")
    parser.add_argument("--host", required=True, help="the Host header NGINX routes on")
    parser.add_argument("--path", default="/v1/predict")
    parser.add_argument("--body", required=True, help="a JSON file holding one request body")
    parser.add_argument("--rate", type=int, default=10, help="requests per second")
    parser.add_argument("--seconds", type=int, default=60)
    return parser.parse_args()


async def main() -> int:
    args = arguments()
    body = json.loads(Path(args.body).read_text())
    this_second: collections.Counter = collections.Counter()
    totals: collections.Counter = collections.Counter()

    async with httpx.AsyncClient(
        base_url=args.url, headers={"Host": args.host}, timeout=35
    ) as http:

        async def send() -> None:
            try:
                response = await http.post(args.path, json=body)
                outcome = "ok" if response.is_success else f"http {response.status_code}"
                version = response.headers.get("x-app-version", "?")
            except httpx.HTTPError as error:
                outcome, version = type(error).__name__, "-"
            this_second[(outcome, version)] += 1
            totals[outcome] += 1

        pending: set[asyncio.Task] = set()
        started = time.monotonic()
        for sent in range(1, args.seconds * args.rate + 1):
            task = asyncio.create_task(send())
            pending.add(task)
            task.add_done_callback(pending.discard)
            await asyncio.sleep(max(0.0, started + sent / args.rate - time.monotonic()))
            if sent % args.rate == 0:
                counts = "  ".join(f"{o} [{v}]: {n}" for (o, v), n in sorted(this_second.items()))
                print(f"{sent // args.rate:4d}s  {counts}", flush=True)
                this_second.clear()
        if pending:
            await asyncio.gather(*pending)

    print("total:", dict(totals))
    return 0 if set(totals) <= {"ok"} else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
