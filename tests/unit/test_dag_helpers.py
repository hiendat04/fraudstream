"""Unit tests for how Airflow's tasks get their credentials: from Vault-backed connections."""

from __future__ import annotations

import json
import os
import re
from unittest import TestCase, main
from unittest.mock import patch

from fraudstream_airflow import dag_helpers
from fraudstream_airflow.dag_helpers import CREDENTIALS_ENV, DEFAULT_ARGS, STORAGE_KWARGS, verify_source_manifest


class CredentialsTest(TestCase):
    """Every credential a task reads is a template over a connection Airflow takes from Vault."""

    def test_every_credential_comes_from_a_vault_connection(self) -> None:
        for name, template in {**CREDENTIALS_ENV, **STORAGE_KWARGS}.items():
            self.assertRegex(template, r"^\{\{ conn\.(postgres|minio|lakefs|pgvector|gateway)\.(login|password) \}\}$", name)

    def test_the_credentials_cover_what_the_jobs_read(self) -> None:
        self.assertEqual(set(CREDENTIALS_ENV), {
            "POSTGRES_USER", "POSTGRES_PASSWORD", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY",
            "LAKEFS_ACCESS_KEY", "LAKEFS_SECRET_KEY",
            "PGVECTOR_USER", "PGVECTOR_PASSWORD", "GATEWAY_USER", "GATEWAY_PASSWORD",
        })

    def test_tasks_keep_their_own_environment(self) -> None:
        self.assertIs(DEFAULT_ARGS["append_env"], True)
        self.assertIs(DEFAULT_ARGS["env"], CREDENTIALS_ENV)

    def test_the_source_manifest_gate_reads_minio_with_the_connection_keys(self) -> None:
        environment = {"FRAUDSTREAM_WAREHOUSE_URI": "s3a://bucket/warehouse", "MINIO_ENDPOINT": "http://minio:9000",
                       "LAKEFS_ENDPOINT": "http://lakefs:8000"}
        with patch.dict(os.environ, environment, clear=True), \
                patch.object(dag_helpers, "validate_source_manifest", return_value={"file_count": 1}) as gate:
            result = verify_source_manifest("/data/raw", minio_access_key="ak", minio_secret_key="sk",
                                            lakefs_access_key="lak", lakefs_secret_key="lsk")
        self.assertEqual(result, {"file_count": 1})
        source_dir, warehouse = gate.call_args.args[0], gate.call_args.kwargs["warehouse"]
        self.assertEqual(source_dir, "/data/raw")
        self.assertEqual((warehouse.uri, warehouse.endpoint, warehouse.access_key, warehouse.secret_key),
                         ("s3a://bucket/warehouse", "http://minio:9000", "ak", "sk"))
        self.assertEqual(warehouse.s3_settings("fraudstream-raw"), ("http://lakefs:8000", "lak", "lsk"))

    def test_spark_commands_carry_the_conf_variable(self) -> None:
        command = dag_helpers.spark_job_command("fraudstream.jobs.silver.transactions", "--write-mode overwrite")

        self.assertIn("python -m fraudstream.jobs.silver.transactions --write-mode overwrite", command)
        self.assertIn("var.json.get('fraudstream_spark_conf', {})", command)
        self.assertIn("--spark-conf", command)

        dags = os.path.join(os.path.dirname(__file__), "..", "..", "airflow", "dags")
        for module in (
            "fraudstream.jobs.bronze.ingest_transactions",
            "fraudstream.jobs.silver.transactions",
            "fraudstream.jobs.gold.transactions",
            "fraudstream.jobs.gold.offline_features",
            "fraudstream.jobs.gold.transaction_labels",
        ):
            with self.subTest(module=module):
                sources = "".join(
                    open(os.path.join(dags, name), encoding="utf-8").read()
                    for name in os.listdir(dags)
                    if name.endswith(".py")
                )
                self.assertIn(f'spark_job_command(\n                "{module}"', sources)

    def test_no_dag_reads_a_password_from_the_scheduler_environment(self) -> None:
        dags = os.path.join(os.path.dirname(__file__), "..", "..", "airflow", "dags")
        for name in os.listdir(dags):
            if name.endswith(".py"):
                with open(os.path.join(dags, name), encoding="utf-8") as dag:
                    self.assertIsNone(re.search(r"PASSWORD|SECRET_KEY", dag.read()), name)

    def test_dags_read_shared_values_from_airflow(self) -> None:
        root = os.path.join(os.path.dirname(__file__), "..", "..", "airflow")
        with open(os.path.join(root, "config", "variables.json"), encoding="utf-8") as variables:
            defined = set(json.load(variables))
        dags = os.path.join(root, "dags")
        for name in sorted(os.listdir(dags)):
            if not name.endswith(".py"):
                continue
            with self.subTest(dag=name), open(os.path.join(dags, name), encoding="utf-8") as dag:
                source = dag.read()
                used = set(re.findall(r"var\.value\.(\w+)", source)) | set(re.findall(r"var\.json\.get\('(\w+)'", source))
                self.assertLessEqual(used, defined, "a Variable the DAG reads is missing from variables.json")
                # Hosts, deploy paths and data folders belong in Variables, so a new environment changes no DAG.
                self.assertIsNone(re.search(r"https?://|localhost|/opt/airflow|project_root }}/data/", source))


class MaintenanceDagTest(TestCase):
    def test_maintenance_dag_runs_compaction_then_expiry(self) -> None:
        dag, tasks = _load_dag_with_fake_airflow("lakehouse_maintenance.py")

        self.assertEqual(
            (dag["dag_id"], dag["schedule"], dag["catchup"]), ("fraudstream_lakehouse_maintenance", "0 2 * * *", False)
        )
        self.assertEqual(list(tasks), ["compact_tables", "expire_snapshots"])
        self.assertEqual(tasks["compact_tables"].downstream, [tasks["expire_snapshots"]])
        for task in tasks.values():
            with self.subTest(task=task.task_id):
                self.assertEqual(task.pool, dag_helpers.SPARK_POOL)
                self.assertIn("python -m fraudstream.jobs.maintenance.iceberg_tables", task.bash_command)
                self.assertIn("--spark-conf", task.bash_command)
        for task_id, action, other in (("compact_tables", "compact", "expire"), ("expire_snapshots", "expire", "compact")):
            with self.subTest(task=task_id):
                command = tasks[task_id].bash_command
                tables = re.findall(rf"--{action} (\S+)", command)
                self.assertEqual(len([table for table in tables if table.startswith("iceberg.streaming.")]), 3)
                self.assertEqual(len([table for table in tables if table.startswith("iceberg.gold.feat_")]), 3)
                self.assertNotIn(f"--{other} ", command)
                self.assertNotIn("--zorder", command)


class _FakeTask:
    def __init__(self, registry: dict[str, _FakeTask], task_id: str, bash_command: str, pool: str | None = None) -> None:
        self.task_id, self.bash_command, self.pool, self.downstream = task_id, bash_command, pool, []
        registry[task_id] = self

    def __rshift__(self, other: _FakeTask) -> _FakeTask:
        self.downstream.append(other)
        return other


def _load_dag_with_fake_airflow(file_name: str) -> tuple[dict[str, object], dict[str, _FakeTask]]:
    """Run a DAG file against stand-ins for the Airflow classes it uses, which aren't installed here."""

    import runpy
    from contextlib import nullcontext
    from types import ModuleType

    dag: dict[str, object] = {}
    tasks: dict[str, _FakeTask] = {}
    sdk, bash, pendulum = ModuleType("airflow.sdk"), ModuleType("airflow.providers.standard.operators.bash"), ModuleType("pendulum")
    pendulum.datetime = lambda *args, **kwargs: None  # type: ignore[attr-defined]
    sdk.DAG = lambda **kwargs: dag.update(kwargs) or nullcontext()  # type: ignore[attr-defined]
    bash.BashOperator = lambda **kwargs: _FakeTask(tasks, **kwargs)  # type: ignore[attr-defined]
    path = os.path.join(os.path.dirname(__file__), "..", "..", "airflow", "dags", file_name)
    with patch.dict("sys.modules", {"airflow.sdk": sdk, "airflow.providers.standard.operators.bash": bash, "pendulum": pendulum}):
        runpy.run_path(path, run_name="dag_under_test")
    return dag, tasks


if __name__ == "__main__":
    main()
