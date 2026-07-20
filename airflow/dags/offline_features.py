"""Build and validate point-in-time offline feature tables from core Gold."""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

from fraudstream_airflow.assets import (
    CORE_GOLD_TRANSACTIONS_VALIDATED,
    OFFLINE_FEATURES_VALIDATED,
)
from fraudstream_airflow.dag_helpers import DEFAULT_ARGS, SPARK_POOL, project_python_command
from fraudstream.orchestration.validation import (
    validate_core_gold_summary,
    validate_offline_feature_summary,
)


with DAG(
    dag_id="fraudstream_offline_features",
    description="Compute and validate offline fraud features from persisted Gold facts.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=[CORE_GOLD_TRANSACTIONS_VALIDATED],
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fraudstream", "batch", "features"],
) as dag:
    with TaskGroup(group_id="ingest", tooltip="Read core Gold and materialize feature tables") as ingest:
        verify_core_gold_ready = PythonOperator(
            task_id="verify_core_gold_ready",
            python_callable=validate_core_gold_summary,
            op_kwargs={
                "gold_dir": "{{ var.value.fraudstream_gold_dir }}",
                "silver_summary_path": "{{ var.value.fraudstream_silver_transactions_dir }}/_silver_transactions_summary.json",
                "expected_scope": "core",
            },
            retries=0,
        )

        build_offline_features = BashOperator(
            task_id="build_offline_features",
            pool=SPARK_POOL,
            bash_command=project_python_command(
                "fraudstream.jobs.gold.offline_features",
                """
                --gold-dir "{{ var.value.fraudstream_gold_dir }}"
                --master "{{ var.value.fraudstream_spark_master }}"
                --write-mode "{{ var.value.fraudstream_write_mode }}"
                --processed-at "{{ (dag_run.logical_date or dag_run.start_date).isoformat() }}"
                """,
            ),
        )

        verify_core_gold_ready >> build_offline_features

    with TaskGroup(group_id="validate", tooltip="Check feature outputs and transaction grain") as validate:
        PythonOperator(
            task_id="publish_validated_features",
            python_callable=validate_offline_feature_summary,
            op_kwargs={"gold_dir": "{{ var.value.fraudstream_gold_dir }}"},
            outlets=[OFFLINE_FEATURES_VALIDATED],
            retries=0,
        )

    ingest >> validate
