"""Index what the copilot may cite, every day: the handbook, a card per model version, the feature dictionary.

Each task runs one step of `fraudstream_rag.steps` and leaves its output in the
run's folder. Nothing reaches a store until every document passes validation,
and only chunks whose text or embedding model changed are written. The last task
runs even after a failure, so DataHub shows which check failed; `run_failed` then
fails the run, which Airflow would otherwise judge by that last task alone.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from fraudstream_airflow.dag_helpers import DEFAULT_ARGS, SPARK_POOL, rag_command

ROOT = "{{ var.value.fraudstream_project_root }}"
# Manual runs have no logical date in Airflow 3, so the folder is named after the run id.
RUN_DIR = "{{ var.value.fraudstream_rag_run_dir }}/{{ dag_run.run_id | replace(':', '') | replace('+', '') }}"
RAG_REPO = f"{ROOT}/rag/feature_repo"


with DAG(
    dag_id="fraudstream_rag_index",
    description="Load, validate, chunk and embed the copilot's documents; write Iceberg and pgvector; check retrieval.",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fraudstream", "rag", "llm"],
) as dag:
    load = BashOperator(
        task_id="load_documents",
        bash_command=rag_command(
            "load",
            f"""
            --run-dir "{RUN_DIR}"
            --handbook "{{{{ var.value.fraudstream_rag_handbook_uri }}}}"
            --mlflow "$MLFLOW_TRACKING_URI"
            --feast-repo "{{{{ var.value.fraudstream_feast_repo_path }}}}"
            """,
        ),
    )

    validate = BashOperator(
        task_id="validate_documents",
        # A document that fails validation fails the same way on a retry.
        retries=0,
        bash_command=rag_command("validate", f'--run-dir "{RUN_DIR}"'),
    )

    chunk = BashOperator(task_id="chunk_documents", bash_command=rag_command("chunk", f'--run-dir "{RUN_DIR}"'))

    embed = BashOperator(task_id="embed_chunks", bash_command=rag_command("embed", f'--run-dir "{RUN_DIR}"'))

    write_offline = BashOperator(
        task_id="write_offline",
        # The Iceberg merge starts a local Spark session, like the batch jobs.
        pool=SPARK_POOL,
        bash_command=rag_command("write-offline", f'--run-dir "{RUN_DIR}"'),
    )

    apply_feature_repo = BashOperator(
        task_id="apply_feature_repo",
        bash_command=(
            "set -euo pipefail\n"
            f'cd "{RAG_REPO}"\n'
            f'export PYTHONPATH="{ROOT}/rag/src"\n'
            # The Iceberg table is the view's source; checking it would start Spark for nothing.
            "feast apply --skip-source-validation\n"
        ),
    )

    write_online = BashOperator(
        task_id="write_online",
        bash_command=rag_command("write-online", f'--run-dir "{RUN_DIR}" --repo "{RAG_REPO}"'),
    )

    check_retrieval = BashOperator(
        task_id="check_retrieval",
        retries=0,
        bash_command=rag_command(
            "check",
            f'--run-dir "{RUN_DIR}" --repo "{RAG_REPO}" --golden-question "{ROOT}/rag/checks/golden_questions.yaml"',
        ),
    )

    publish_governance = BashOperator(
        task_id="publish_governance",
        trigger_rule="all_done",
        retries=0,
        # Exit code 99 marks the task skipped: DataHub runs only while governance is being captured.
        bash_command=(
            rag_command(
                "summarize",
                f'--run-dir "{RUN_DIR}" --out "{{{{ var.value.fraudstream_rag_summary_path }}}}"',
            )
            + 'curl -sf -m 5 "$DATAHUB_GMS_URL/health" > /dev/null '
            + '|| { echo "DataHub is not running: the summary is written, governance is not published"; exit 99; }\n'
            + f'export PYTHONPATH="{ROOT}/datahub/src"\n'
            + "/home/airflow/datahub/bin/python -m fraudstream_datahub.publish "
            + f'--project-root "{ROOT}" --pipeline fraudstream_rag_index '
            + '--server "$DATAHUB_GMS_URL" --ui-url http://localhost:18081\n'
        ),
    )

    run_failed = BashOperator(
        task_id="run_failed",
        trigger_rule="one_failed",
        retries=0,
        bash_command='echo "an earlier task failed"; exit 1',
    )

    steps = [load, validate, chunk, embed, write_offline, apply_feature_repo, write_online, check_retrieval]
    steps >> run_failed
    (
        load
        >> validate
        >> chunk
        >> embed
        >> write_offline
        >> apply_feature_repo
        >> write_online
        >> check_retrieval
        >> publish_governance
    )
