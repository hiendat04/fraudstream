"""Each DP1 run writes the raw zone on its own lakeFS branch, and main changes only by a merge."""

from __future__ import annotations

import io
import json
import re
from pathlib import Path
from unittest import TestCase, main
from unittest.mock import patch
from urllib.error import HTTPError

from fraudstream_airflow import lakefs

RAW_URI = "s3a://fraudstream-raw/main/offline_transactions"
RUN_ID = "manual__2026-11-13T10:15:00.123456+00:00"
KEYS = {"lakefs_access_key": "lak", "lakefs_secret_key": "lsk"}


class _Response(io.BytesIO):
    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _answer(body: dict | None = None) -> _Response:
    return _Response(json.dumps(body or {}).encode())


class BranchNamesTest(TestCase):
    def test_a_run_id_becomes_a_branch_name_lakefs_accepts(self) -> None:
        self.assertRegex(lakefs.branch_name(RUN_ID), r"^dp1-[\w-]+$")

    def test_a_raw_path_moves_to_the_run_branch(self) -> None:
        self.assertEqual(lakefs.on_branch(RAW_URI, RUN_ID),
                         f"s3a://fraudstream-raw/{lakefs.branch_name(RUN_ID)}/offline_transactions")


class BranchCallsTest(TestCase):
    def _calls(self, urlopen) -> list[tuple[str, str, dict]]:
        return [(call.args[0].get_method(), call.args[0].full_url, json.loads(call.args[0].data))
                for call in urlopen.call_args_list]

    def test_the_branch_starts_from_the_branch_the_raw_path_names(self) -> None:
        with patch.dict("os.environ", {"LAKEFS_ENDPOINT": "http://lakefs:8000"}), \
                patch.object(lakefs, "urlopen", return_value=_answer()) as urlopen:
            branch = lakefs.create_branch(RAW_URI, RUN_ID, **KEYS)

        self.assertEqual(branch, lakefs.branch_name(RUN_ID))
        self.assertEqual(self._calls(urlopen), [(
            "POST", "http://lakefs:8000/api/v1/repositories/fraudstream-raw/branches",
            {"name": branch, "source": "main"},
        )])
        self.assertTrue(urlopen.call_args.args[0].get_header("Authorization").startswith("Basic "))

    def test_a_retried_run_reuses_its_branch(self) -> None:
        conflict = HTTPError("url", 409, "Conflict", {}, io.BytesIO(b'{"message": "branch already exists"}'))
        with patch.object(lakefs, "urlopen", side_effect=conflict):
            self.assertEqual(lakefs.create_branch(RAW_URI, RUN_ID, **KEYS), lakefs.branch_name(RUN_ID))

    def test_another_error_stops_the_task_with_lakefs_message(self) -> None:
        refused = HTTPError("url", 401, "Unauthorized", {}, io.BytesIO(b'{"message": "error authenticating request"}'))
        with patch.object(lakefs, "urlopen", side_effect=refused):
            with self.assertRaisesRegex(RuntimeError, "401.*error authenticating request"):
                lakefs.create_branch(RAW_URI, RUN_ID, **KEYS)

    def test_the_branch_is_committed_then_merged_into_main(self) -> None:
        answers = [_answer({"id": "branch-commit"}), _answer({"reference": "merge-commit"})]
        with patch.dict("os.environ", {"LAKEFS_ENDPOINT": "http://lakefs:8000"}), \
                patch.object(lakefs, "urlopen", side_effect=answers) as urlopen:
            merged = lakefs.commit_and_merge(RAW_URI, RUN_ID, **KEYS)

        branch = lakefs.branch_name(RUN_ID)
        (commit_method, commit_url, commit), (merge_method, merge_url, merge) = self._calls(urlopen)
        self.assertEqual(merged, "merge-commit", "Bronze records the commit main moved to")
        self.assertEqual((commit_method, commit_url),
                         ("POST", f"http://lakefs:8000/api/v1/repositories/fraudstream-raw/branches/{branch}/commits"))
        self.assertEqual(commit["metadata"], {"airflow_run_id": RUN_ID})
        self.assertEqual((merge_method, merge_url),
                         ("POST", f"http://lakefs:8000/api/v1/repositories/fraudstream-raw/refs/{branch}/merge/main"))
        self.assertTrue(commit["allow_empty"] and merge["allow_empty"], "a rerun with the same data still merges")


class RawToBronzeDagTest(TestCase):
    def setUp(self) -> None:
        self.dag = (Path(__file__).parents[2] / "airflow" / "dags" / "raw_to_bronze.py").read_text(encoding="utf-8")

    def test_main_changes_only_after_the_check(self) -> None:
        self.assertIn("create_branch >> generate_raw_source >> verify_source_manifest >> commit_and_merge >> ingest_bronze",
                      self.dag)

    def test_the_generator_writes_to_the_run_branch(self) -> None:
        for variable in ("fraudstream_raw_uri", "fraudstream_labels_uri"):
            with self.subTest(variable=variable):
                self.assertIn(f'--{variable.removeprefix("fraudstream_").replace("_", "-")} '
                              f'"{{{{ on_branch(var.value.{variable}, run_id) }}}}"', self.dag)

    def test_bronze_records_the_merged_commit(self) -> None:
        self.assertRegex(self.dag, r"--source-commit \"\{\{ ti\.xcom_pull\(task_ids='ingest\.commit_and_merge'\) \}\}\"")


if __name__ == "__main__":
    main()
