"""Shared Airflow DAG settings and command construction."""

from __future__ import annotations

import os
from datetime import timedelta
from textwrap import dedent
from typing import Any

from fraudstream.jobs.warehouse import DEFAULT_MINIO_ENDPOINT, DEFAULT_WAREHOUSE_URI, WarehouseConfig
from fraudstream.orchestration.validation import validate_source_manifest


SPARK_POOL = "fraudstream_spark"

# The jobs read their credentials from the environment. Airflow fills these in from
# connections it reads from Vault, and masks the passwords in logs and rendered fields.
CREDENTIALS_ENV = {
    "POSTGRES_USER": "{{ conn.postgres.login }}",
    "POSTGRES_PASSWORD": "{{ conn.postgres.password }}",
    "MINIO_ACCESS_KEY": "{{ conn.minio.login }}",
    "MINIO_SECRET_KEY": "{{ conn.minio.password }}",
    "PGVECTOR_USER": "{{ conn.pgvector.login }}",
    "PGVECTOR_PASSWORD": "{{ conn.pgvector.password }}",
    "GATEWAY_USER": "{{ conn.gateway.login }}",
    "GATEWAY_PASSWORD": "{{ conn.gateway.password }}",
}
# A Python task has no environment of its own to fill, so it gets MinIO's keys as arguments.
MINIO_KWARGS = {
    "minio_access_key": "{{ conn.minio.login }}",
    "minio_secret_key": "{{ conn.minio.password }}",
}

DEFAULT_ARGS = {
    "owner": "fraudstream",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    # Only BashOperators take these; append_env keeps the rest of the worker's environment.
    "env": CREDENTIALS_ENV,
    "append_env": True,
}


def verify_source_manifest(source_dir: str, *, minio_access_key: str, minio_secret_key: str) -> dict[str, Any]:
    """The source manifest's quality gate, with MinIO's keys from the minio connection."""

    warehouse = WarehouseConfig(
        uri=os.environ.get("FRAUDSTREAM_WAREHOUSE_URI", DEFAULT_WAREHOUSE_URI),
        endpoint=os.environ.get("MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT),
        access_key=minio_access_key,
        secret_key=minio_secret_key,
    )
    return validate_source_manifest(source_dir, warehouse=warehouse)


def project_python_command(module: str, arguments: str) -> str:
    """Return a BashOperator command that uses Airflow-managed project paths."""

    normalized_arguments = " ".join(
        line.strip() for line in arguments.splitlines() if line.strip()
    )
    return dedent(
        f"""\
        set -euo pipefail
        cd "{{{{ var.value.fraudstream_project_root }}}}"
        export PYTHONPATH="{{{{ var.value.fraudstream_project_root }}}}/src"
        python -m {module} {normalized_arguments}
        """
    )


def rag_command(step: str, arguments: str) -> str:
    """Run one step of the RAG pipeline, with the lakehouse and Feast helpers it reuses on the path."""

    normalized_arguments = " ".join(
        line.strip() for line in arguments.splitlines() if line.strip()
    )
    root = "{{ var.value.fraudstream_project_root }}"
    return dedent(
        f"""\
        set -euo pipefail
        cd "{root}"
        export PYTHONPATH="{root}/rag/src:{root}/feature_store/src:{root}/src"
        python -m fraudstream_rag.steps {step} {normalized_arguments}
        """
    )


def monitoring_command(module: str, arguments: str) -> str:
    """Like project_python_command, with the drift check and the code it reuses on the path."""

    normalized_arguments = " ".join(
        line.strip() for line in arguments.splitlines() if line.strip()
    )
    root = "{{ var.value.fraudstream_project_root }}"
    return dedent(
        f"""\
        set -euo pipefail
        cd "{root}"
        export PYTHONPATH="{root}/monitoring/src:{root}/api/src:{root}/ml/src:{root}/src"
        python -m {module} {normalized_arguments}
        """
    )
