"""Check every day whether the model's inputs have drifted, publish it, and retrain on drift.

There is no ground truth to compare with: fraud is confirmed weeks later. So the
check compares the newest week of the offline feature store with the rows the
served model was trained on, input by input, using the drift API's PSI.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import DAG, Param

from fraudstream_airflow.dag_helpers import DEFAULT_ARGS, SPARK_POOL, monitoring_command
from fraudstream_airflow.monitoring import drift_found

# Manual runs have no logical date in Airflow 3, so the file is named after the run id.
SUMMARY = (
    "{{ var.value.fraudstream_drift_summary_dir }}/"
    "{{ dag_run.run_id | replace(':', '') | replace('+', '') }}.json"
)


with DAG(
    dag_id="fraudstream_drift_monitoring",
    description="Compare the newest features with the training data, publish PSI to Grafana, retrain on drift.",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "window_days": Param(7, type="integer", minimum=1, maximum=90, description="How many days to check."),
        "window_end": Param(None, type=["null", "string"], format="date", description="mm/dd/yyyy, not included. Empty: the day after the newest payment."),
        "force_retrain": Param(False, type="boolean", description="Start the training pipeline even without drift."),
    },
    tags=["fraudstream", "monitoring", "ml"],
) as dag:
    check_drift = BashOperator(
        task_id="check_drift",
        # Feast's offline store starts a local Spark session, like the batch jobs.
        pool=SPARK_POOL,
        bash_command=monitoring_command(
            "fraudstream_monitoring.drift",
            f"""
            --repo-path "{{{{ var.value.fraudstream_feast_repo_path }}}}"
            --reference "{{{{ var.value.fraudstream_drift_reference_path }}}}"
            --window-days "{{{{ params.window_days }}}}"
            --window-end "{{{{ params.window_end or '' }}}}"
            --out "{SUMMARY}"
            """,
        ),
    )

    publish_metrics = BashOperator(
        task_id="publish_metrics",
        bash_command=monitoring_command("fraudstream_monitoring.publish", f'--summary "{SUMMARY}"'),
    )

    decide = ShortCircuitOperator(
        task_id="drift_found",
        python_callable=drift_found,
        op_kwargs={"summary_path": SUMMARY},
    )

    trigger_retrain = BashOperator(
        task_id="trigger_retrain",
        # A retry could start a second training run.
        retries=0,
        bash_command=monitoring_command(
            "fraudstream_monitoring.retrain",
            f"""--summary "{SUMMARY}" {{{{ '--force' if params.force_retrain else '' }}}}""",
        ),
    )

    check_drift >> publish_metrics >> decide >> trigger_retrain
