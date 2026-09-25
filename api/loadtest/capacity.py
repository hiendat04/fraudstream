"""The same load, raised by 10 users a minute up to 120, to find where the SLA breaks.

    cd api && set -a && . ../.env && set +a
    uv run --group loadtest locust -f loadtest/capacity.py --headless \
        --html ../reports/load_test_inference_api_capacity.html
"""

from locust import LoadTestShape

from locustfile import InferenceUser, check_sla  # noqa: F401  registers the user and the check

STEP_USERS = 10
STEP_SECONDS = 60
MOST_USERS = 120


class StepLoad(LoadTestShape):
    def tick(self):
        users = STEP_USERS * (int(self.get_run_time() // STEP_SECONDS) + 1)
        return (users, users) if users <= MOST_USERS else None
