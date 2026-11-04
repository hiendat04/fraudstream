"""Measure four ways of joining skewed transactions to their dimensions.

Half of all transactions are in New York and 40% are `online_marketplace`, so a
plain shuffle join sends those rows to one task each while the other tasks sit
idle. The benchmark joins `silver.stg_transactions` to `gold.dim_city` and
`gold.dim_merchant_category` once per strategy, writes nothing (`noop`), and
records how long it took and how uneven the join's tasks were:

    baseline   plain shuffle join; adaptive execution and broadcast off
    adaptive   adaptive execution splits the oversized partitions
    salted     a random salt spreads each key over several tasks
    broadcast  the small dimension is copied to every task, so nothing is shuffled

The spread comes from Spark's own REST API, the numbers behind the Stages tab.

Run from the repo root, inside the Airflow container, once per strategy:

    docker compose exec -w /opt/airflow/project -e MINIO_ACCESS_KEY -e MINIO_SECRET_KEY -e POSTGRES_USER -e POSTGRES_PASSWORD airflow-scheduler python -m fraudstream.jobs.gold.skew_benchmark --strategy baseline --spark-ui-retain-seconds 600
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

from fraudstream.jobs.gold.transactions import _build_spark_session
from fraudstream.jobs.spark_settings import add_spark_conf_argument, parse_spark_conf
from fraudstream.jobs.spark_ui import (
    SparkUIConfig,
    add_spark_ui_arguments,
    announce_spark_ui,
    retain_spark_ui,
)
from fraudstream.jobs.warehouse import (
    add_iceberg_arguments,
    add_postgres_arguments,
    add_warehouse_arguments,
    iceberg_config_from_args,
    postgres_config_from_args,
    warehouse_config_from_args,
)

STRATEGIES = ("baseline", "adaptive", "salted", "broadcast")
DEFAULT_OUTPUT_DIR = Path("data/benchmarks")
DEFAULT_BUCKETS = 16
SALT_SEED = 7
# The same number of shuffle partitions for every strategy, so only the skew handling differs.
SHUFFLE_PARTITIONS = "200"
NO_BROADCAST = "-1"


def strategy_conf(strategy: str) -> dict[str, str]:
    """The Spark settings one strategy runs with."""

    settings = {
        "baseline": {"spark.sql.adaptive.enabled": "false", "spark.sql.autoBroadcastJoinThreshold": NO_BROADCAST},
        "adaptive": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.autoBroadcastJoinThreshold": NO_BROADCAST,
        },
        "salted": {"spark.sql.adaptive.enabled": "false", "spark.sql.autoBroadcastJoinThreshold": NO_BROADCAST},
        "broadcast": {"spark.sql.adaptive.enabled": "true"},
    }
    if strategy not in settings:
        raise ValueError(f"strategy must be one of {', '.join(STRATEGIES)}, got {strategy!r}")
    return {**settings[strategy], "spark.sql.shuffle.partitions": SHUFFLE_PARTITIONS}


def salted_join(left: Any, right: Any, key: str, buckets: int, how: str = "left") -> Any:
    """Join on `key`, spreading each key's left rows over `buckets` tasks.

    Each left row gets a random salt in 0..buckets-1; each right row is repeated
    once per salt, so every left row still meets its match. The salt is dropped.
    """

    from pyspark.sql import functions as F

    salted_left = left.withColumn("_salt", F.floor(F.rand(SALT_SEED) * buckets).cast("int"))
    salted_right = right.withColumn("_salt", F.explode(F.sequence(F.lit(0), F.lit(buckets - 1))))
    return salted_left.join(salted_right, [key, "_salt"], how).drop("_salt")


def stage_spread(stage_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    """How uneven the heaviest stage was: its median task against its slowest one.

    The heaviest stage is the one that read the most shuffle data, which is the join
    itself; without a shuffle (the broadcast join) it is the one that read the most input.
    """

    stage = max(stage_summaries, key=lambda item: (item["shuffleReadBytes"], item["inputBytes"]))
    summary = stage["taskSummary"]
    records = (
        summary["shuffleReadMetrics"]["readRecords"] if stage["shuffleReadBytes"] else summary["inputMetrics"]["recordsRead"]
    )
    return {
        "stage_id": stage["stageId"],
        "tasks": stage["numTasks"],
        "median_task_ms": int(summary["executorRunTime"][0]),
        "max_task_ms": int(summary["executorRunTime"][1]),
        "median_records": int(records[0]),
        "max_records": int(records[1]),
    }


def run_benchmark(spark: Any, strategy: str, buckets: int, catalog: str) -> float:
    """Join transactions to the city and category dimensions with one strategy; return the seconds it took."""

    from pyspark.sql import functions as F

    def join(left: Any, right: Any, key: str) -> Any:
        if strategy == "salted":
            return salted_join(left, right, key, buckets)
        if strategy == "broadcast":
            return left.join(F.broadcast(right), key, "left")
        return left.join(right, key, "left")

    transactions = spark.table(f"{catalog}.silver.stg_transactions")
    cities = spark.table(f"{catalog}.gold.dim_city").select("city", "city_key")
    categories = spark.table(f"{catalog}.gold.dim_merchant_category").select("merchant_category", "merchant_category_key")
    joined = join(join(transactions, cities, "city"), categories, "merchant_category")

    spark.sparkContext.setJobDescription(f"Skew benchmark: {strategy}")
    started = time.monotonic()
    joined.write.format("noop").mode("overwrite").save()
    return round(time.monotonic() - started, 1)


def _stage_summaries(spark: Any) -> list[dict[str, Any]]:
    """Every completed stage from Spark's REST API, each with its median and maximum task metrics."""

    base = f"{spark.sparkContext.uiWebUrl}/api/v1/applications/{spark.sparkContext.applicationId}"
    stages = _get_json(f"{base}/stages?status=complete")
    for stage in stages:
        stage["taskSummary"] = _get_json(f"{base}/stages/{stage['stageId']}/{stage['attemptId']}/taskSummary?quantiles=0.5,1.0")
    return stages


def _get_json(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=30) as response:
        return json.load(response)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare four ways of joining skewed transactions to their dimensions.")
    parser.add_argument("--strategy", choices=STRATEGIES, required=True)
    parser.add_argument("--buckets", type=int, default=DEFAULT_BUCKETS, help="Salt values per key, for --strategy salted.")
    parser.add_argument("--master", default="local[*]")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    add_spark_ui_arguments(parser)
    add_spark_conf_argument(parser)
    add_warehouse_arguments(parser)
    add_iceberg_arguments(parser)
    add_postgres_arguments(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run one strategy and write data/benchmarks/skew_<strategy>.json."""

    args = build_parser().parse_args(argv)
    iceberg = iceberg_config_from_args(args)
    conf = {
        **strategy_conf(args.strategy),
        "spark.app.name": f"skew-benchmark-{args.strategy}",
        **parse_spark_conf(args.spark_conf),
    }
    # The spread is read from the Spark UI's REST API, so the UI is always on.
    spark_ui = SparkUIConfig(enabled=True, port=args.spark_ui_port, retain_seconds=args.spark_ui_retain_seconds)
    spark = _build_spark_session(
        args.master, spark_ui, warehouse_config_from_args(args), iceberg, postgres_config_from_args(args), conf
    )
    try:
        announce_spark_ui(spark, spark_ui)
        seconds = run_benchmark(spark, args.strategy, args.buckets, iceberg.catalog_name)
        result = {
            "strategy": args.strategy,
            "buckets": args.buckets if args.strategy == "salted" else None,
            "seconds": seconds,
            "join_stage": stage_spread(_stage_summaries(spark)),
            "spark_conf": conf,
        }
        retain_spark_ui(spark, spark_ui)
    finally:
        spark.stop()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / f"skew_{args.strategy}.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2), file=sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
