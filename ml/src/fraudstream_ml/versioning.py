"""Save each training set as a new version of one Iceberg table.

Every run merges its rows into the table, matching on transaction id. Iceberg
only stores the rows that changed. It reuses the files that did not. So a second
run that adds one more month of data costs one month, not a whole new copy.

Each run gets back a snapshot id. Read the table at that snapshot, and you get
back exactly the rows that run used.
"""

from __future__ import annotations

from typing import Any

TRAINING_TABLE = "iceberg.ml.training_data"
KEY_COLUMN = "transaction_id"
TIMESTAMP_COLUMN = "event_timestamp"


def _namespace_of(table: str) -> str:

    return table.rsplit(".", 1)[0]


def ensure_table(spark: Any, source: Any, table: str = TRAINING_TABLE) -> None:
    """Create the table the first time it is needed, using the shape of the data.

    Split by month, so adding a new month only touches that month's files.
    Set to merge-on-read, so changing a row writes a small marker instead of
    rewriting whole files.
    """

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_namespace_of(table)}")
    if spark.catalog.tableExists(table):
        return

    columns = ", ".join(
        f"`{field.name}` {field.dataType.simpleString()}" for field in source.schema.fields
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} ({columns})
        USING iceberg
        PARTITIONED BY (months(`{TIMESTAMP_COLUMN}`))
        TBLPROPERTIES (
            'write.merge.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read'
        )
        """
    )


def write_snapshot(spark: Any, frame: Any, table: str = TRAINING_TABLE) -> int:
    """Add the rows to the table and return the snapshot id that now holds them.

    Rows that are already there with the same values are left alone. Without
    that check, every run would rewrite every row it matched.
    """

    source = spark.createDataFrame(frame) if not hasattr(frame, "sparkSession") else frame
    ensure_table(spark, source, table)

    view = "incoming_training_rows"
    source.createOrReplaceTempView(view)

    comparable = [column for column in source.columns if column != KEY_COLUMN]
    changed = " OR ".join(f"t.`{column}` IS DISTINCT FROM s.`{column}`" for column in comparable)

    spark.sql(
        f"""
        MERGE INTO {table} t
        USING {view} s
        ON t.`{KEY_COLUMN}` = s.`{KEY_COLUMN}`
        WHEN MATCHED AND ({changed}) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.catalog.dropTempView(view)
    return current_snapshot_id(spark, table)


def current_snapshot_id(spark: Any, table: str = TRAINING_TABLE) -> int:
    """Return the snapshot the table is currently on."""

    row = spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).first()
    if row is None:
        raise ValueError(f"{table} has no snapshots yet")
    return int(row["snapshot_id"])


def read_at_snapshot(spark: Any, snapshot_id: int, table: str = TRAINING_TABLE) -> Any:
    """Read the table as it was at one snapshot."""

    return spark.read.option("snapshot-id", snapshot_id).format("iceberg").load(table)


def added_records(spark: Any, table: str, snapshot_id: int) -> int:
    """Return how many rows one commit wrote."""

    row = spark.sql(
        f"SELECT summary['added-records'] AS added FROM {table}.snapshots "
        f"WHERE snapshot_id = {snapshot_id}"
    ).first()
    return int(row["added"] or 0)


def snapshot_history(spark: Any, table: str = TRAINING_TABLE) -> list[dict[str, Any]]:
    """Return every commit in order, with how many rows it added and the new total."""

    rows = spark.sql(
        f"""
        SELECT snapshot_id, committed_at,
               summary['added-records'] AS added_records,
               summary['total-records'] AS total_records
        FROM {table}.snapshots
        ORDER BY committed_at
        """
    ).collect()
    return [
        {
            "snapshot_id": int(row["snapshot_id"]),
            "committed_at": row["committed_at"],
            "added_records": int(row["added_records"] or 0),
            "total_records": int(row["total_records"] or 0),
        }
        for row in rows
    ]
