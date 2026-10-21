"""Load test for the inference API. The HTML report is the SLA record.

Each user sends one payment a second, so the number of users is the request
rate. Warm up first: the autoscaler needs about 50 seconds, and a run shorter
than that measures one overloaded pod.

    cd api && set -a && . ../.env && set +a
    uv run --group loadtest locust -f loadtest/locustfile.py \
        --headless -u 50 -r 50 -t 5m --html ../reports/load_test_inference_api.html
"""

import json
import os
from pathlib import Path

from locust import HttpUser, constant_throughput, events, task

from sla import check_sla as report

PAYMENT = json.loads((Path(__file__).parents[1] / "tools" / "transaction.json").read_text())
SLA = {"p95_ms": 500, "p99_ms": 1000, "failure_ratio": 0.01}


class InferenceUser(HttpUser):
    """One client scoring one payment a second through the gateway."""

    host = "https://inference.fraudstream.localhost"
    wait_time = constant_throughput(1)

    def on_start(self):
        self.client.auth = (os.environ["GATEWAY_USER"], os.environ["GATEWAY_PASSWORD"])
        self.client.verify = str(Path(__file__).parents[2] / "local-ca.crt")

    @task
    def predict(self):
        self.client.post("/v1/predict", json=PAYMENT, name="POST /v1/predict")


@events.quitting.add_listener
def check_sla(environment, **_):
    report(environment, SLA)
