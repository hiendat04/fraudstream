"""One `--spark-conf KEY=VALUE` flag for every Spark job, so a run's settings come from outside the code.

A baseline run and a tuned run use the same job; only their settings differ. Each
setting is applied last, after the job's own defaults, so it always wins, and the
job writes what it applied into its summary.

    PYTHONPATH=src uv run --extra spark --extra storage python -m fraudstream.jobs.silver.transactions --spark-conf spark.sql.adaptive.enabled=false --spark-conf spark.sql.shuffle.partitions=200
"""

from __future__ import annotations

import argparse
from typing import Any, Sequence


def add_spark_conf_argument(parser: argparse.ArgumentParser) -> None:
    """Add the repeatable `--spark-conf KEY=VALUE` flag to one job parser."""

    parser.add_argument(
        "--spark-conf",
        action="append",
        default=[],
        type=_checked_pair,
        metavar="KEY=VALUE",
        help="A Spark setting applied after the job's defaults; repeat for several.",
    )


def parse_spark_conf(values: Sequence[str]) -> dict[str, str]:
    """Turn `KEY=VALUE` strings into settings. Spark ignores unknown keys silently, so non-`spark.` keys are refused."""

    conf: dict[str, str] = {}
    for value in values:
        key, separator, setting = value.partition("=")
        key = key.strip()
        if not separator:
            raise ValueError(f"--spark-conf needs KEY=VALUE, got {value!r}")
        if not key.startswith("spark."):
            raise ValueError(f"--spark-conf keys start with 'spark.', got {key!r}")
        conf[key] = setting.strip()
    return conf


def _checked_pair(value: str) -> str:
    """Reject a bad pair while parsing, so the job stops with a usage error before Spark starts."""

    try:
        parse_spark_conf([value])
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    return value


def apply_spark_conf(builder: Any, conf: dict[str, str]) -> Any:
    """Set each setting on a SparkSession builder; call it last so it overrides the job's defaults."""

    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder
