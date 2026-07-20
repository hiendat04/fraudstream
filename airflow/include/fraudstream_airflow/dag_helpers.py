"""Shared Airflow DAG settings and command construction."""

from __future__ import annotations

from datetime import timedelta
from textwrap import dedent


SPARK_POOL = "fraudstream_spark"
DEFAULT_ARGS = {
    "owner": "fraudstream",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}


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
