"""The Iceberg table, on a real Iceberg catalog in a temporary folder.

The thing being tested is what each run commits, which a fake table cannot show.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import pytest

from fraudstream_rag.offline import CHUNK_COLUMNS, write_offline

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"


@pytest.fixture(scope="module")
def spark(tmp_path_factory: pytest.TempPathFactory):
    from pyspark.sql import SparkSession

    # Spark starts its Python workers with `python3` from PATH unless told otherwise.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    warehouse = tmp_path_factory.mktemp("warehouse").as_uri()
    session = (
        SparkSession.builder.master("local[2]")
        .appName("RagOfflineTest")
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def frame(rows: list[tuple[str, str]], day: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "chunk_id": [chunk_id for chunk_id, _ in rows],
            "doc_id": "handbook/page",
            "source": "handbook",
            "title": "Page",
            "heading": [chunk_id for chunk_id, _ in rows],
            "text": [f"text {digest}" for _, digest in rows],
            "content_hash": [digest for _, digest in rows],
            "embedding": [[0.6, 0.8]] * len(rows),
            "updated_at": pd.Timestamp(day, tz="UTC"),
        },
        columns=list(CHUNK_COLUMNS),
    )


def snapshots(spark, table: str) -> int:
    return spark.sql(f"SELECT count(*) AS n FROM {table}.snapshots").first()["n"]


def stored(spark, table: str) -> dict[str, tuple[str, str]]:
    # Cast in Spark, which uses the session's UTC; collect() would turn it into local time.
    rows = spark.sql(f"SELECT chunk_id, content_hash, CAST(updated_at AS STRING) AS updated_at FROM {table}").collect()
    return {row["chunk_id"]: (row["content_hash"], row["updated_at"]) for row in rows}


def test_each_run_commits_only_what_changed(spark) -> None:
    table = "iceberg.rag.chunks_runs"

    first = write_offline(spark, frame([("a", "h1"), ("b", "h2"), ("c", "h3")], "2026-10-01"), table)
    assert (first["rows"], first["written"], first["deleted"]) == (3, 3, 0)
    assert snapshots(spark, table) == 1

    again = write_offline(spark, frame([("a", "h1"), ("b", "h2"), ("c", "h3")], "2026-10-02"), table)
    assert (again["written"], again["deleted"], again["snapshot_id"]) == (0, 0, first["snapshot_id"])
    assert snapshots(spark, table) == 1

    changed = write_offline(spark, frame([("a", "h1"), ("b", "h2-new"), ("d", "h4")], "2026-10-03"), table)
    assert (changed["rows"], changed["written"], changed["deleted"]) == (3, 2, 1)
    assert stored(spark, table) == {
        "a": ("h1", "2026-10-01 00:00:00"),
        "b": ("h2-new", "2026-10-03 00:00:00"),
        "d": ("h4", "2026-10-03 00:00:00"),
    }
    assert snapshots(spark, table) == 2


def test_embeddings_are_stored_as_float_arrays(spark) -> None:
    table = "iceberg.rag.chunks_types"
    rows = frame([("a", "h1")], "2026-10-01")
    rows["embedding"] = [pd.array([0.6, 0.8], dtype="float32").to_numpy()]

    write_offline(spark, rows, table)

    assert dict(spark.table(table).dtypes)["embedding"] == "array<float>"
    assert [round(value, 4) for value in spark.table(table).first()["embedding"]] == [0.6, 0.8]
