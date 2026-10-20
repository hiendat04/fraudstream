"""The offline copy of every chunk: one Iceberg table, merged on chunk id.

Iceberg keeps a snapshot per change, so the table can say what the copilot knew on
any day, by time travel. A run that changes nothing commits nothing, so every
snapshot is a real change to what the copilot knows.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from fraudstream_rag.changes import plan_changes

CHUNKS_TABLE = "iceberg.rag.chunks"
CHUNK_SCHEMA = (
    "chunk_id STRING, doc_id STRING, source STRING, title STRING, heading STRING, "
    "text STRING, content_hash STRING, embedding ARRAY<FLOAT>, updated_at TIMESTAMP"
)
CHUNK_COLUMNS = tuple(column.split()[0] for column in CHUNK_SCHEMA.split(", "))


def plain_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """The chunk columns, with each embedding as a list of Python floats, which Spark and Feast both take."""

    rows = frame[list(CHUNK_COLUMNS)].copy()
    rows["embedding"] = rows["embedding"].map(lambda vector: [float(value) for value in vector])
    return rows


def write_offline(spark: Any, frame: pd.DataFrame, table: str = CHUNKS_TABLE) -> dict[str, Any]:
    """Make the table hold exactly these chunks, and say what changed."""

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({CHUNK_SCHEMA}) USING iceberg")
    stored = {
        row["chunk_id"]: row["content_hash"]
        for row in spark.table(table).select("chunk_id", "content_hash").collect()
    }
    changes = plan_changes(stored, dict(zip(frame["chunk_id"], frame["content_hash"])))
    if not changes.empty:
        spark.createDataFrame(plain_rows(frame), schema=CHUNK_SCHEMA).createOrReplaceTempView("incoming_chunks")
        spark.sql(
            f"""
            MERGE INTO {table} t
            USING incoming_chunks s
            ON t.chunk_id = s.chunk_id
            WHEN MATCHED AND t.content_hash <> s.content_hash THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            WHEN NOT MATCHED BY SOURCE THEN DELETE
            """
        )
        spark.catalog.dropTempView("incoming_chunks")
    snapshot = spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).first()
    return {
        "table": table,
        "rows": spark.table(table).count(),
        "written": len(changes.write),
        "deleted": len(changes.delete),
        "snapshot_id": None if snapshot is None else int(snapshot["snapshot_id"]),
    }
