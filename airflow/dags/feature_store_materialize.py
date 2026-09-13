"""Incrementally materialize offline features into the Feast online store."""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

from fraudstream_airflow.assets import FEATURE_STORE_MATERIALIZED, OFFLINE_FEATURES_VALIDATED
from fraudstream_airflow.dag_helpers import DEFAULT_ARGS, SPARK_POOL
from fraudstream.orchestration.validation import validate_feature_store_materialization


with DAG(
    dag_id="fraudstream_feature_store_materialize",
    description="Apply the Feast repo and materialize offline features into the online store.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=[OFFLINE_FEATURES_VALIDATED],
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fraudstream", "feature-store", "ml"],
) as dag:
    with TaskGroup(group_id="materialize", tooltip="Apply definitions and load the online store") as materialize:
        apply_feature_repo = BashOperator(
            task_id="apply_feature_repo",
            # `feast apply` validates every source with a `SELECT *`, which
            # starts its own local Spark session -- it needs the same pool as
            # the task below to avoid running unmanaged alongside the batch
            # Spark jobs that `fraudstream_spark_pool_slots` serializes.
            pool=SPARK_POOL,
            bash_command=(
                "set -euo pipefail\n"
                'cd "{{ var.value.fraudstream_feast_repo_path }}"\n'
                "feast apply\n"
            ),
        )

        materialize_features = BashOperator(
            task_id="materialize_features",
            # Feast's Spark offline store starts a local Spark session, so this
            # shares the pool that already serializes the batch Spark jobs.
            pool=SPARK_POOL,
            bash_command=(
                "set -euo pipefail\n"
                'cd "{{ var.value.fraudstream_project_root }}"\n'
                'export PYTHONPATH="{{ var.value.fraudstream_project_root }}/src:'
                '{{ var.value.fraudstream_project_root }}/feature_store/src"\n'
                "python -m fraudstream_feast.materialize "
                '--repo-path "{{ var.value.fraudstream_feast_repo_path }}" '
                '--start-timestamp "{{ var.value.fraudstream_feature_store_start_timestamp }}" '
                '--end-timestamp "{{ (dag_run.logical_date or dag_run.start_date).isoformat() }}" '
                '--summary-path "{{ var.value.fraudstream_feature_store_summary_path }}"\n'
            ),
        )

        apply_feature_repo >> materialize_features

    with TaskGroup(group_id="validate", tooltip="Check the online store answers a real read") as validate:
        PythonOperator(
            task_id="publish_materialized_features",
            python_callable=validate_feature_store_materialization,
            op_kwargs={"summary_path": "{{ var.value.fraudstream_feature_store_summary_path }}"},
            outlets=[FEATURE_STORE_MATERIALIZED],
            retries=0,
        )

    materialize >> validate
