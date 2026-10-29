"""Unit tests for how Airflow's tasks get their credentials: from Vault-backed connections."""

from __future__ import annotations

import os
import re
from unittest import TestCase, main
from unittest.mock import patch

from fraudstream_airflow import dag_helpers
from fraudstream_airflow.dag_helpers import CREDENTIALS_ENV, DEFAULT_ARGS, MINIO_KWARGS, verify_source_manifest


class CredentialsTest(TestCase):
    """Every credential a task reads is a template over a connection Airflow takes from Vault."""

    def test_every_credential_comes_from_a_vault_connection(self) -> None:
        for name, template in {**CREDENTIALS_ENV, **MINIO_KWARGS}.items():
            self.assertRegex(template, r"^\{\{ conn\.(postgres|minio|pgvector|gateway)\.(login|password) \}\}$", name)

    def test_the_credentials_cover_what_the_jobs_read(self) -> None:
        self.assertEqual(set(CREDENTIALS_ENV), {
            "POSTGRES_USER", "POSTGRES_PASSWORD", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY",
            "PGVECTOR_USER", "PGVECTOR_PASSWORD", "GATEWAY_USER", "GATEWAY_PASSWORD",
        })

    def test_tasks_keep_their_own_environment(self) -> None:
        self.assertIs(DEFAULT_ARGS["append_env"], True)
        self.assertIs(DEFAULT_ARGS["env"], CREDENTIALS_ENV)

    def test_the_source_manifest_gate_reads_minio_with_the_connection_keys(self) -> None:
        environment = {"FRAUDSTREAM_WAREHOUSE_URI": "s3a://bucket/warehouse", "MINIO_ENDPOINT": "http://minio:9000"}
        with patch.dict(os.environ, environment, clear=True), \
                patch.object(dag_helpers, "validate_source_manifest", return_value={"file_count": 1}) as gate:
            result = verify_source_manifest("/data/raw", minio_access_key="ak", minio_secret_key="sk")
        self.assertEqual(result, {"file_count": 1})
        source_dir, warehouse = gate.call_args.args[0], gate.call_args.kwargs["warehouse"]
        self.assertEqual(source_dir, "/data/raw")
        self.assertEqual((warehouse.uri, warehouse.endpoint, warehouse.access_key, warehouse.secret_key),
                         ("s3a://bucket/warehouse", "http://minio:9000", "ak", "sk"))

    def test_no_dag_reads_a_password_from_the_scheduler_environment(self) -> None:
        dags = os.path.join(os.path.dirname(__file__), "..", "..", "airflow", "dags")
        for name in os.listdir(dags):
            if name.endswith(".py"):
                with open(os.path.join(dags, name), encoding="utf-8") as dag:
                    self.assertIsNone(re.search(r"PASSWORD|SECRET_KEY", dag.read()), name)


if __name__ == "__main__":
    main()
