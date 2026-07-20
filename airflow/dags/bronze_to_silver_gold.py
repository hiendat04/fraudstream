"""Transform validated Bronze transactions into Silver and core Gold tables."""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

from fraudstream_airflow.assets import (
    BRONZE_TRANSACTIONS_VALIDATED,
    CORE_GOLD_TRANSACTIONS_VALIDATED,
)
from fraudstream_airflow.dag_helpers import DEFAULT_ARGS, SPARK_POOL, project_python_command
from fraudstream.orchestration.validation import (
    validate_bronze_report,
    validate_core_gold_summary,
    validate_silver_summary,
)


with DAG(
    dag_id="fraudstream_bronze_to_silver_gold",
    description="Build and validate Silver transactions, then build and validate core Gold.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=[BRONZE_TRANSACTIONS_VALIDATED],
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fraudstream", "batch", "silver", "gold"],
) as dag:
    with TaskGroup(group_id="silver_ingest", tooltip="Build clean, deduplicated Silver") as silver_ingest:
        verify_bronze_ready = PythonOperator(
            task_id="verify_bronze_ready",
            python_callable=validate_bronze_report,
            op_kwargs={
                "report_path": "{{ var.value.fraudstream_bronze_validation_report }}",
            },
            retries=0,
        )

        build_silver = BashOperator(
            task_id="build_silver",
            pool=SPARK_POOL,
            bash_command=project_python_command(
                "fraudstream.jobs.silver.transactions",
                """
                --bronze-dir "{{ var.value.fraudstream_bronze_transactions_dir }}"
                --output-dir "{{ var.value.fraudstream_silver_transactions_dir }}"
                --quality-output-dir "{{ var.value.fraudstream_silver_quality_dir }}"
                --master "{{ var.value.fraudstream_spark_master }}"
                --write-mode "{{ var.value.fraudstream_write_mode }}"
                --processed-at "{{ (dag_run.logical_date or dag_run.start_date).isoformat() }}"
                """,
            ),
        )

        verify_bronze_ready >> build_silver

    with TaskGroup(group_id="silver_validate", tooltip="Check Silver row accounting") as silver_validate:
        PythonOperator(
            task_id="validate_silver",
            python_callable=validate_silver_summary,
            op_kwargs={
                "summary_path": "{{ var.value.fraudstream_silver_transactions_dir }}/_silver_transactions_summary.json",
                "quality_report_path": "{{ var.value.fraudstream_silver_transactions_dir }}/_silver_quality_report.json",
            },
            retries=0,
        )

    with TaskGroup(group_id="gold_ingest", tooltip="Build Gold dimensions and facts") as gold_ingest:
        BashOperator(
            task_id="build_core_gold",
            pool=SPARK_POOL,
            bash_command=project_python_command(
                "fraudstream.jobs.gold.transactions",
                """
                --silver-dir "{{ var.value.fraudstream_silver_transactions_dir }}"
                --output-dir "{{ var.value.fraudstream_gold_dir }}"
                --master "{{ var.value.fraudstream_spark_master }}"
                --write-mode "{{ var.value.fraudstream_write_mode }}"
                --processed-at "{{ (dag_run.logical_date or dag_run.start_date).isoformat() }}"
                --core-only
                """,
            ),
        )

    with TaskGroup(group_id="gold_validate", tooltip="Check Gold tables and row grain") as gold_validate:
        PythonOperator(
            task_id="publish_validated_core_gold",
            python_callable=validate_core_gold_summary,
            op_kwargs={
                "gold_dir": "{{ var.value.fraudstream_gold_dir }}",
                "silver_summary_path": "{{ var.value.fraudstream_silver_transactions_dir }}/_silver_transactions_summary.json",
                "expected_scope": "core",
            },
            outlets=[CORE_GOLD_TRANSACTIONS_VALIDATED],
            retries=0,
        )

    silver_ingest >> silver_validate >> gold_ingest >> gold_validate
