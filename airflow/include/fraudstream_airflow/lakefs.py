"""Each DP1 run writes the raw zone on its own lakeFS branch; main changes only when that branch merges."""

from __future__ import annotations

import base64
import json
import os
import re
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from fraudstream.jobs.warehouse import DEFAULT_LAKEFS_ENDPOINT


def branch_name(run_id: str) -> str:
    """The run's branch: lakeFS allows only letters, digits, `_` and `-` in a name."""

    return "dp1-" + re.sub(r"[^\w-]", "-", run_id)


def on_branch(uri: str, run_id: str) -> str:
    """The same raw path on the run's branch: `s3a://repo/main/x` becomes `s3a://repo/dp1-<run>/x`."""

    repository, _, path = _split(uri)
    return f"s3a://{repository}/{branch_name(run_id)}/{path}"


def create_branch(raw_uri: str, run_id: str, *, lakefs_access_key: str, lakefs_secret_key: str) -> str:
    """Branch the run off the branch `raw_uri` names, usually main."""

    repository, source, _ = _split(raw_uri)
    branch = branch_name(run_id)
    try:
        _post(f"/repositories/{repository}/branches", {"name": branch, "source": source},
              lakefs_access_key, lakefs_secret_key)
    except _LakeFSError as error:
        if error.status != 409:  # 409: a retried run's branch already exists, so reuse it
            raise
    return branch


def commit_and_merge(raw_uri: str, run_id: str, *, lakefs_access_key: str, lakefs_secret_key: str) -> str:
    """Commit what the run wrote and merge it into the branch `raw_uri` names. Returns the merged commit."""

    repository, target, _ = _split(raw_uri)
    branch = branch_name(run_id)
    # allow_empty: a rerun that regenerates identical files still commits and merges.
    _post(f"/repositories/{repository}/branches/{branch}/commits",
          {"message": f"DP1 run {run_id}", "metadata": {"airflow_run_id": run_id}, "allow_empty": True},
          lakefs_access_key, lakefs_secret_key)
    merged = _post(f"/repositories/{repository}/refs/{branch}/merge/{target}",
                   {"message": f"Merge DP1 run {run_id}", "allow_empty": True},
                   lakefs_access_key, lakefs_secret_key)
    return merged["reference"]


class _LakeFSError(RuntimeError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"lakeFS answered {status}: {message}")
        self.status = status


def _split(uri: str) -> tuple[str, str, str]:
    """`s3a://repo/branch/path` as (repo, branch, path)."""

    repository, branch, path = uri.removeprefix("s3a://").split("/", 2)
    return repository, branch, path


def _post(path: str, body: dict[str, Any], access_key: str, secret_key: str) -> dict[str, Any]:
    endpoint = os.environ.get("LAKEFS_ENDPOINT", DEFAULT_LAKEFS_ENDPOINT)
    login = base64.b64encode(f"{access_key}:{secret_key}".encode()).decode()
    request = Request(f"{endpoint}/api/v1{path}", data=json.dumps(body).encode(), method="POST",
                      headers={"Content-Type": "application/json", "Authorization": f"Basic {login}"})
    try:
        with urlopen(request, timeout=60) as response:
            answer = response.read()
    except HTTPError as error:
        raise _LakeFSError(error.code, error.read().decode(errors="replace")) from None
    # Creating a branch answers with plain text, the other calls with JSON.
    return json.loads(answer) if answer.lstrip().startswith(b"{") else {}
