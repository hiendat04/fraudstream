"""Load test for the context API. The HTML report is the SLA record.

Each user makes one request a second, in the mix a copilot makes: three customer
lookups, two merchant lookups and one handbook search in six. Ids come from real
payments; a 404 for an id with no stored history is a right answer, not a failure.

    cd api && set -a && . ../.env && set +a
    uv run --group loadtest locust -f loadtest/context.py \
        --headless -u 20 -r 20 -t 5m --html ../reports/load_test_context_api.html
"""

import json
import os
import random
from pathlib import Path

from locust import HttpUser, constant_throughput, events, task

from sla import check_sla

PAYMENTS = [
    json.loads(line)
    for line in (Path(__file__).parents[1] / "tools" / "payments.jsonl").read_text().splitlines()
]
QUESTIONS = [
    "What do I do when I suspect card testing?",
    "How long do we have to respond to a chargeback?",
    "What does total_orders_90d mean?",
    "How do we detect account takeover?",
    "When should I block a card?",
]
SLA = {"p95_ms": 500, "p99_ms": 1000, "failure_ratio": 0.01}


class ContextUser(HttpUser):
    """One copilot asking the context API once a second, through the gateway."""

    host = "https://context.fraudstream.localhost"
    wait_time = constant_throughput(1)

    def on_start(self):
        self.client.auth = (os.environ["GATEWAY_USER"], os.environ["GATEWAY_PASSWORD"])
        self.client.verify = str(Path(__file__).parents[2] / "local-ca.crt")

    def _lookup(self, path: str, name: str) -> None:
        with self.client.get(path, name=name, catch_response=True) as response:
            if response.status_code in (200, 404):
                response.success()

    @task(3)
    def customer(self):
        self._lookup(f"/v1/customers/{random.choice(PAYMENTS)['customer_id']}", "GET /v1/customers/{id}")

    @task(2)
    def merchant(self):
        self._lookup(f"/v1/merchants/{random.choice(PAYMENTS)['merchant_id']}", "GET /v1/merchants/{id}")

    @task(1)
    def search(self):
        self.client.post("/v1/search", json={"question": random.choice(QUESTIONS)}, name="POST /v1/search")


@events.quitting.add_listener
def check(environment, **_):
    check_sla(environment, SLA)
