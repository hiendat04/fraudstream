"""Every night, merge the small files the streaming job and the feature builds leave, then drop old snapshots.

Flink commits a few files to each streaming table every checkpoint, so they pile up
fastest. Expiry runs second, because compaction leaves the old files in place for
time travel until their snapshots expire.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from fraudstream_airflow.dag_helpers import DEFAULT_ARGS, SPARK_POOL, spark_job_command

TABLES = (
    "iceberg.streaming.clean_transactions",
    "iceberg.streaming.customer_features_5m",
    "iceberg.streaming.merchant_features_5m",
    "iceberg.gold.feat_customer_rolling",
    "iceberg.gold.feat_customer_total_orders_90d",
    "iceberg.gold.feat_merchant_risk_rolling",
)
TABLE_ARGUMENTS = "\n".join(f"--table {table}" for table in TABLES)
REPORT_DIR = "{{ var.value.fraudstream_project_root }}/data/maintenance"


def maintenance_command(step: str) -> str:
    return spark_job_command(
        "fraudstream.jobs.maintenance.iceberg_tables",
        f"""
        --step {step}
        {TABLE_ARGUMENTS}
        --output-dir "{REPORT_DIR}/{step}"
        --warehouse-uri "{{{{ var.value.fraudstream_warehouse_uri }}}}"
        --master "{{{{ var.value.fraudstream_spark_master }}}}"
        """,
    )


with DAG(
    dag_id="fraudstream_lakehouse_maintenance",
    description="Compact the streaming and feature Iceberg tables, then expire their old snapshots.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fraudstream", "batch", "maintenance"],
) as dag:
    compact_tables = BashOperator(task_id="compact_tables", pool=SPARK_POOL, bash_command=maintenance_command("compact"))
    expire_snapshots = BashOperator(task_id="expire_snapshots", pool=SPARK_POOL, bash_command=maintenance_command("expire"))

    compact_tables >> expire_snapshots
