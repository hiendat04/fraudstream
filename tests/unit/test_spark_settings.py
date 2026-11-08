"""Unit tests for the --spark-conf flag the Spark jobs share."""

from __future__ import annotations

import argparse
import sys
import types
from unittest import TestCase, main
from unittest.mock import patch

from fraudstream.jobs.gold.transaction_labels import TransactionLabelsResult
from fraudstream.jobs.silver import transactions as silver
from fraudstream.jobs.spark_settings import (
    RETAIN_UI_FOREVER_SECONDS,
    add_retain_ui_argument,
    add_spark_conf_argument,
    apply_spark_conf,
    keep_ui_open,
    parse_spark_conf,
)


class _RecordingBuilder:
    """A SparkSession builder stand-in that keeps the last value set for each key."""

    def __init__(self) -> None:
        self.settings: dict[str, str] = {}

    def appName(self, _name: str) -> "_RecordingBuilder":
        return self

    def master(self, _master: str) -> "_RecordingBuilder":
        return self

    def config(self, key: str, value: str) -> "_RecordingBuilder":
        self.settings[key] = value
        return self

    def getOrCreate(self) -> "_RecordingBuilder":
        return self


class SparkSettingsTest(TestCase):
    def test_parse_spark_conf_reads_pairs(self) -> None:
        parser = argparse.ArgumentParser()
        add_spark_conf_argument(parser)
        args = parser.parse_args(
            ["--spark-conf", "spark.sql.shuffle.partitions=800", "--spark-conf", "spark.sql.adaptive.enabled=true"]
        )

        self.assertEqual(
            parse_spark_conf(args.spark_conf),
            {"spark.sql.shuffle.partitions": "800", "spark.sql.adaptive.enabled": "true"},
        )
        self.assertEqual(parse_spark_conf([]), {})

    def test_parse_spark_conf_rejects_missing_equals_and_non_spark_keys(self) -> None:
        with self.assertRaises(ValueError):
            parse_spark_conf(["spark.sql.shuffle.partitions"])
        with self.assertRaises(ValueError):
            parse_spark_conf(["sql.shuffle.partitions=800"])

    def test_conf_overrides_the_job_default(self) -> None:
        builder = _RecordingBuilder()
        fake_pyspark_sql = types.SimpleNamespace(SparkSession=types.SimpleNamespace(builder=builder))

        with patch.dict(sys.modules, {"pyspark.sql": fake_pyspark_sql}):
            silver._build_spark_session("local[1]", spark_conf={"spark.sql.shuffle.partitions": "800"})

        self.assertEqual(builder.settings["spark.sql.shuffle.partitions"], "800")

    def test_apply_spark_conf_keeps_defaults_without_conf(self) -> None:
        builder = _RecordingBuilder().config("spark.sql.shuffle.partitions", "8")

        self.assertEqual(apply_spark_conf(builder, {}).settings["spark.sql.shuffle.partitions"], "8")

    def test_applied_conf_is_written_to_the_summary(self) -> None:
        result = TransactionLabelsResult(
            labels_uri="s3a://fraudstream/raw/transaction_labels",
            iceberg_table="iceberg.gold.transaction_labels",
            row_count=10,
            distinct_transaction_id_count=10,
            fraud_row_count=1,
            write_mode="overwrite",
            spark_version="4.1.0",
            processed_at="2026-11-03T00:00:00Z",
            completed_at="2026-11-03T00:01:00Z",
            spark_conf={"spark.sql.shuffle.partitions": "800"},
        )

        self.assertEqual(result.to_dict()["spark_conf"], {"spark.sql.shuffle.partitions": "800"})


class RetainUiTest(TestCase):
    def _parse(self, *flags: str) -> int:
        parser = argparse.ArgumentParser()
        add_retain_ui_argument(parser)
        return parser.parse_args(flags).retain_ui

    def test_retain_ui_takes_seconds_or_forever(self) -> None:
        self.assertEqual(self._parse("--retain-ui", "600"), 600)
        self.assertEqual(self._parse("--retain-ui", "forever"), RETAIN_UI_FOREVER_SECONDS)

    def test_ctrl_c_ends_the_hold_and_lets_the_job_finish(self) -> None:
        with patch("fraudstream.jobs.spark_settings.time.sleep", side_effect=KeyboardInterrupt):
            keep_ui_open(None, RETAIN_UI_FOREVER_SECONDS)


if __name__ == "__main__":
    main()
