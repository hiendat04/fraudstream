"""Generate raw offline transactions, ingest Bronze, and reconcile the result.

The generator writes to the run's own lakeFS branch. Main changes only when the
checked branch merges, so a stopped or failed run leaves main as it was and its
branch in place to inspect.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

from fraudstream_airflow import lakefs
from fraudstream_airflow.assets import BRONZE_TRANSACTIONS_VALIDATED
from fraudstream_airflow.dag_helpers import (
    DEFAULT_ARGS,
    LAKEFS_KWARGS,
    STORAGE_KWARGS,
    SPARK_POOL,
    project_python_command,
    spark_job_command,
    verify_source_manifest,
)
from fraudstream.orchestration.validation import (
    validate_bronze_report,
)


with DAG(
    dag_id="fraudstream_raw_to_bronze",
    description="Generate source files, ingest Bronze Parquet, and reconcile raw preservation.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    user_defined_macros={"on_branch": lakefs.on_branch},
    tags=["fraudstream", "batch", "bronze"],
) as dag:
    with TaskGroup(group_id="ingest", tooltip="Create and ingest raw source data") as ingest:
        create_branch = PythonOperator(
            task_id="create_branch",
            python_callable=lakefs.create_branch,
            op_kwargs={"raw_uri": "{{ var.value.fraudstream_raw_uri }}", "run_id": "{{ run_id }}", **LAKEFS_KWARGS},
        )

        generate_raw_source = BashOperator(
            task_id="generate_raw_source",
            bash_command=project_python_command(
                "fraudstream.generators.offline_transactions",
                """
                --config "{{ var.value.fraudstream_offline_generator_config }}"
                --output-dir "{{ var.value.fraudstream_raw_transactions_dir }}"
                --raw-uri "{{ on_branch(var.value.fraudstream_raw_uri, run_id) }}"
                --labels-uri "{{ on_branch(var.value.fraudstream_labels_uri, run_id) }}"
                --workers "{{ var.value.fraudstream_generator_workers }}"
                """,
            ),
        )

        verify_source_manifest = PythonOperator(
            task_id="verify_source_manifest",
            python_callable=verify_source_manifest,
            op_kwargs={
                "source_dir": "{{ var.value.fraudstream_raw_transactions_dir }}",
                **STORAGE_KWARGS,
            },
        )

        commit_and_merge = PythonOperator(
            task_id="commit_and_merge",
            python_callable=lakefs.commit_and_merge,
            op_kwargs={"raw_uri": "{{ var.value.fraudstream_raw_uri }}", "run_id": "{{ run_id }}", **LAKEFS_KWARGS},
        )

        ingest_bronze = BashOperator(
            task_id="ingest_bronze",
            pool=SPARK_POOL,
            bash_command=spark_job_command(
                "fraudstream.jobs.bronze.ingest_transactions",
                """
                --source-dir "{{ var.value.fraudstream_raw_transactions_dir }}"
                --source-uri "{{ var.value.fraudstream_raw_uri }}"
                --output-dir "{{ var.value.fraudstream_bronze_transactions_dir }}"
                --manifest-path "{{ var.value.fraudstream_raw_transactions_dir }}/_manifest.json"
                --warehouse-uri "{{ var.value.fraudstream_warehouse_uri }}"
                --master "{{ var.value.fraudstream_spark_master }}"
                --postgres-days "{{ var.value.fraudstream_postgres_days_bronze_silver }}"
                --write-mode "{{ var.value.fraudstream_write_mode }}"
                --ingest-run-id "{{ run_id }}"
                --source-commit "{{ ti.xcom_pull(task_ids='ingest.commit_and_merge') }}"
                --ingest-date "{{ (dag_run.logical_date or dag_run.start_date).strftime('%Y-%m-%d') }}"
                """,
            ),
        )

        create_branch >> generate_raw_source >> verify_source_manifest >> commit_and_merge >> ingest_bronze

    with TaskGroup(group_id="validate", tooltip="Reconcile Bronze with the raw source") as validate:
        reconcile_bronze = BashOperator(
            task_id="reconcile_bronze",
            pool=SPARK_POOL,
            retries=0,
            bash_command=project_python_command(
                "fraudstream.jobs.bronze.validate_transactions",
                """
                --source-dir "{{ var.value.fraudstream_raw_transactions_dir }}"
                --source-uri "{{ var.value.fraudstream_raw_uri }}"
                --warehouse-uri "{{ var.value.fraudstream_warehouse_uri }}"
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
