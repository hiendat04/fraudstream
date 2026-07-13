"""Shared helpers for observing local FraudStream jobs in the Spark UI."""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from typing import Any


DEFAULT_SPARK_UI_PORT = 4040
DEFAULT_SPARK_UI_RETAIN_SECONDS = 0


@dataclass(frozen=True)
class SparkUIConfig:
    """Optional live Spark UI settings shared by the offline jobs."""

    enabled: bool = False
    port: int = DEFAULT_SPARK_UI_PORT
    retain_seconds: int = DEFAULT_SPARK_UI_RETAIN_SECONDS

    def validate(self) -> None:
        """Raise when a Spark UI setting is outside its supported range."""

        if not 1 <= self.port <= 65_535:
            raise ValueError("spark_ui_port must be between 1 and 65535")
        if self.retain_seconds < 0:
            raise ValueError("spark_ui_retain_seconds must be zero or greater")


def add_spark_ui_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the common live Spark UI arguments to one job parser."""

    parser.add_argument(
        "--spark-ui",
        action="store_true",
        help="Enable the live Spark UI for this job.",
    )
    parser.add_argument(
        "--spark-ui-port",
        type=int,
        default=DEFAULT_SPARK_UI_PORT,
        help=f"Preferred Spark UI port. Defaults to {DEFAULT_SPARK_UI_PORT}.",
    )
    parser.add_argument(
        "--spark-ui-retain-seconds",
        type=int,
        default=DEFAULT_SPARK_UI_RETAIN_SECONDS,
        help="Keep the completed job's live UI open for this many seconds before Spark stops.",
    )


def spark_ui_config_from_args(args: argparse.Namespace) -> SparkUIConfig:
    """Build and validate Spark UI settings from parsed CLI arguments."""

    config = SparkUIConfig(
        enabled=args.spark_ui,
        port=args.spark_ui_port,
        retain_seconds=args.spark_ui_retain_seconds,
    )
    config.validate()
    return config


def configure_spark_builder(builder: Any, config: SparkUIConfig) -> Any:
    """Apply live UI settings to a SparkSession builder."""

    config.validate()
    configured_builder = builder.config("spark.ui.enabled", str(config.enabled).lower())
    if config.enabled:
        configured_builder = configured_builder.config("spark.ui.port", str(config.port))
    return configured_builder


def announce_spark_ui(spark: Any, config: SparkUIConfig) -> str | None:
    """Print and return the actual Spark UI URL when observation is enabled."""

    if not config.enabled:
        return None

    ui_url = spark.sparkContext.uiWebUrl
    if ui_url:
        print(f"Spark UI: {ui_url}", file=sys.stderr, flush=True)
    else:
        print("Spark UI was enabled, but Spark did not expose a UI URL.", file=sys.stderr, flush=True)
    return ui_url


def set_spark_job_group(spark: Any, group_id: str, description: str) -> None:
    """Give the next Spark actions a readable name in the Jobs page."""

    spark.sparkContext.setJobGroup(group_id, description, interruptOnCancel=True)
    spark.sparkContext.setJobDescription(description)


def clear_spark_job_group(spark: Any) -> None:
    """Remove the current job-group labels after a logical action finishes."""

    spark_context = spark.sparkContext
    clear_job_group = getattr(spark_context, "clearJobGroup", None)
    if clear_job_group is not None:
        clear_job_group()
    else:
        spark_context.setLocalProperty("spark.jobGroup.id", None)
        spark_context.setLocalProperty("spark.job.description", None)


def retain_spark_ui(spark: Any, config: SparkUIConfig) -> None:
    """Keep the live UI available briefly after the final Spark action."""

    if not config.enabled or config.retain_seconds == 0:
        return

    ui_url = spark.sparkContext.uiWebUrl or f"http://localhost:{config.port}"
    print(
        f"Spark processing is complete. Keeping {ui_url} open for "
        f"{config.retain_seconds} seconds for inspection.",
        file=sys.stderr,
        flush=True,
    )
    time.sleep(config.retain_seconds)
