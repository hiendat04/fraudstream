"""Score one payment through the inference API, then ask the model directly with the same inputs.

Both answers must match. The API is reached through NGINX, the model through
Kourier, so this also checks the two front doors.

    cd api && set -a && . ../.env && set +a
    POSTGRES_HOST=localhost REDIS_HOST=localhost PYTHONPATH=src:../ml/src \
        uv run python tools/check_prediction.py
"""

import asyncio
import json
import sys
from pathlib import Path

import httpx

from fraudstream_api.inference.features import model_inputs, usable_features
from fraudstream_api.inference.online_store import FeastReader
from fraudstream_api.inference.schemas import Transaction
from fraudstream_api.inference.settings import Settings

API = "http://localhost/v1/predict"
API_HOST = "inference.localhost"
MODEL = "http://localhost:8081/v1/models/fraud-detection:predict"
MODEL_HOST = "fraud-detection-predictor.kserve-models.knative.localhost"


async def main() -> int:
    payment = json.loads(Path(__file__).with_name("transaction.json").read_text())

    reader = FeastReader(Settings())
    await reader.start()
    transaction = Transaction(**payment)
    online = await reader.read(transaction.customer_id, transaction.merchant_id)
    inputs = model_inputs(transaction, usable_features(online, transaction.event_timestamp, reader.ttls()))
    await reader.close()

    async with httpx.AsyncClient(timeout=60) as http:
        served = (await http.post(API, json=payment, headers={"Host": API_HOST})).json()
        direct = (
            await http.post(
                MODEL, json={"instances": [inputs]}, headers={"Host": MODEL_HOST}
            )
        ).json()["predictions"][0]

    print(f"  through the inference API: {served['fraud_probability']}")
    print(f"  model asked directly:      {direct}")
    print(f"  history found:             {served['history_found']}")
    same = abs(served["fraud_probability"] - direct) < 1e-9
    print("  same answer" if same else "  DIFFERENT ANSWERS")
    return 0 if same else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
