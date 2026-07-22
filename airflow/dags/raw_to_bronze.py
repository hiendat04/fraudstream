"""Generate raw offline transactions, ingest Bronze, and reconcile the result."""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

from fraudstream_airflow.assets import BRONZE_TRANSACTIONS_VALIDATED
from fraudstream_airflow.dag_helpers import DEFAULT_ARGS, SPARK_POOL, project_python_command
from fraudstream.orchestration.validation import (
    validate_bronze_report,
    validate_source_manifest,
)


with DAG(
    dag_id="fraudstream_raw_to_bronze",
    description="Generate source files, ingest Bronze Parquet, and reconcile raw preservation.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fraudstream", "batch", "bronze"],
) as dag:
    with TaskGroup(group_id="ingest", tooltip="Create and ingest raw source data") as ingest:
        generate_raw_source = BashOperator(
            task_id="generate_raw_source",
            bash_command=project_python_command(
                "fraudstream.generators.offline_transactions",
                """
                --config "{{ var.value.fraudstream_offline_generator_config }}"
                --output-dir "{{ var.value.fraudstream_raw_transactions_dir }}"
                """,
            ),
        )

        verify_source_manifest = PythonOperator(
            task_id="verify_source_manifest",
            python_callable=validate_source_manifest,
            op_kwargs={
                "source_dir": "{{ var.value.fraudstream_raw_transactions_dir }}",
                "project_root": "{{ var.value.fraudstream_project_root }}",
            },
        )

        ingest_bronze = BashOperator(
            task_id="ingest_bronze",
            pool=SPARK_POOL,
            bash_command=project_python_command(
                "fraudstream.jobs.bronze.ingest_transactions",
                """
                --source-dir "{{ var.value.fraudstream_raw_transactions_dir }}"
                --output-dir "{{ var.value.fraudstream_bronze_transactions_dir }}"
                --manifest-path "{{ var.value.fraudstream_raw_transactions_dir }}/_manifest.json"
                --master "{{ var.value.fraudstream_spark_master }}"
                --write-mode "{{ var.value.fraudstream_write_mode }}"
                --ingest-run-id "{{ run_id }}"
                --ingest-date "{{ (dag_run.logical_date or dag_run.start_date).strftime('%Y-%m-%d') }}"
                """,
            ),
        )

        generate_raw_source >> verify_source_manifest >> ingest_bronze

    with TaskGroup(group_id="validate", tooltip="Reconcile Bronze with the raw source") as validate:
        reconcile_bronze = BashOperator(
            task_id="reconcile_bronze",
            pool=SPARK_POOL,
            retries=0,
            bash_command=project_python_command(
                "fraudstream.jobs.bronze.validate_transactions",
                """
                --source-dir "{{ var.value.fraudstream_raw_transactions_dir }}"
                --bronze-dir "{{ var.value.fraudstream_bronze_transactions_dir }}"
                --master "{{ var.value.fraudstream_spark_master }}"
                --report-path "{{ var.value.fraudstream_bronze_validation_report }}"
                """,
            ),
        )

        publish_validated_bronze = PythonOperator(
            task_id="publish_validated_bronze",
            python_callable=validate_bronze_report,
            op_kwargs={
                "report_path": "{{ var.value.fraudstream_bronze_validation_report }}",
            },
            outlets=[BRONZE_TRANSACTIONS_VALIDATED],
            retries=0,
        )

        reconcile_bronze >> publish_validated_bronze

    ingest >> validate
