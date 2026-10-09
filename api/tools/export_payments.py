"""Export a sample of the newest week of real payments, as request bodies.

The A/B test replays them, so both model versions see a realistic mix.

    cd api && PYTHONPATH=src:../ml/src uv run --group reference \\
        python tools/export_payments.py --count 3000
"""

import argparse
import json
import random
from pathlib import Path

FACT_TABLE = "iceberg.gold.fact_transactions"
OUT = Path(__file__).with_name("payments.jsonl")


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--count", type=int, default=3000)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=18082)
    parser.add_argument("--seed", type=int, default=13)
    return parser.parse_args()


def main() -> None:
    import trino

    from fraudstream_ml.features import CHANNELS, CITIES

    args = arguments()
    query = (
        "SELECT transaction_id, customer_id, merchant_dim_id, event_time, "
        "CAST(amount AS DOUBLE), channel, city "
        f"FROM {FACT_TABLE} "
        f"WHERE event_time >= (SELECT max(event_time) FROM {FACT_TABLE}) "
        f"- INTERVAL '{int(args.days)}' DAY"
    )
    connection = trino.dbapi.connect(
        host=args.host, port=args.port, user="fraudstream", catalog="iceberg"
    )
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
    finally:
        connection.close()

    # The API refuses unknown channels and cities and amounts of zero; Gold should hold none.
    rows = [row for row in rows if row[5] in CHANNELS and row[6] in CITIES and row[4] > 0]
    picked = random.Random(args.seed).sample(rows, min(args.count, len(rows)))
    picked.sort(key=lambda row: row[3])
    with OUT.open("w") as out:
        for transaction_id, customer_id, merchant_id, event_time, amount, channel, city in picked:
            payment = {
                "transaction_id": transaction_id,
                "customer_id": customer_id,
                "merchant_id": merchant_id,
                "event_timestamp": event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "amount": round(amount, 2),
                "channel": channel,
                "city": city,
            }
            out.write(json.dumps(payment) + "\n")
    print(f"{len(picked):,} of {len(rows):,} payments from the newest {args.days} days -> {OUT}")


if __name__ == "__main__":
    main()
