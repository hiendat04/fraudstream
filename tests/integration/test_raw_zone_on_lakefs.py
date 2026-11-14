"""DP1's raw-zone steps against a real lakeFS in a throwaway container: branch, generate, check, merge.

Needs Docker; skipped without it. Run with `./cicd/test.sh integration`.
"""

from __future__ import annotations

import base64
import csv
import json
import os
import secrets
import time
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import SkipTest, TestCase, main
from unittest.mock import patch
from urllib.error import URLError
from urllib.request import Request, urlopen

from fraudstream import storage
from fraudstream.generators.offline_transactions import OfflineGeneratorConfig, generate_offline_transactions
from fraudstream.jobs.warehouse import WarehouseConfig
from fraudstream.orchestration.validation import validate_source_manifest
from fraudstream_airflow import lakefs

LAKEFS_IMAGE = "treeverse/lakefs:1.88.0"
RAW_URI = "s3a://fraudstream-raw/main/offline_transactions"
LABELS_URI = "s3a://fraudstream-raw/main/transaction_labels"


class RawZoneOnLakeFSTest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            from testcontainers.core.container import DockerContainer
            import docker

            docker.from_env().ping()
        except Exception as error:  # no package, or no Docker daemon
            raise SkipTest(f"Docker isn't available: {error}") from None

        # Made per run, so no key is ever written down.
        cls.keys = {"lakefs_access_key": "AKIA" + secrets.token_hex(8).upper(),
                    "lakefs_secret_key": secrets.token_hex(24)}
        cls.lakefs = (
            DockerContainer(LAKEFS_IMAGE)
            .with_exposed_ports(8000)
            .with_env("LAKEFS_DATABASE_TYPE", "local")
            .with_env("LAKEFS_BLOCKSTORE_TYPE", "local")
            .with_env("LAKEFS_AUTH_ENCRYPT_SECRET_KEY", secrets.token_hex(24))
            .with_env("LAKEFS_INSTALLATION_USER_NAME", "admin")
            .with_env("LAKEFS_INSTALLATION_ACCESS_KEY_ID", cls.keys["lakefs_access_key"])
            .with_env("LAKEFS_INSTALLATION_SECRET_ACCESS_KEY", cls.keys["lakefs_secret_key"])
        )
        cls.lakefs.start()
        cls.addClassCleanup(cls.lakefs.stop)
        cls.endpoint = f"http://{cls.lakefs.get_container_host_ip()}:{cls.lakefs.get_exposed_port(8000)}"
        _wait_until_healthy(cls.endpoint)
        _api(cls.endpoint, cls.keys, "/repositories",
             {"name": "fraudstream-raw", "storage_namespace": "local://fraudstream-raw", "default_branch": "main"})

        environment = patch.dict(os.environ, {"LAKEFS_ENDPOINT": cls.endpoint})
        environment.start()
        cls.addClassCleanup(environment.stop)
        cls.warehouse = WarehouseConfig(lakefs_endpoint=cls.endpoint, **cls.keys)

    def test_a_run_reaches_main_only_after_it_merges(self) -> None:
        run_id = "manual__integration-full-run"
        lakefs.create_branch(RAW_URI, run_id, **self.keys)
        main_before = self._files_on_main()

        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            summary = generate_offline_transactions(_two_days(
                output_dir, lakefs.on_branch(RAW_URI, run_id), lakefs.on_branch(LABELS_URI, run_id), self.warehouse))
            self.assertEqual(self._files_on_main(), main_before, "the generator wrote to its branch, not to main")

            checked = validate_source_manifest(output_dir, warehouse=self.warehouse)
            self.assertEqual(checked["file_count"], 2)

        merged = lakefs.commit_and_merge(RAW_URI, run_id, **self.keys)
        self.assertRegex(merged, r"^[0-9a-f]{64}$")

        on_main = [uri for uri in self._files_on_main() if uri not in main_before]
        self.assertEqual(len(on_main), 2, "both days reached main with the merge")
        rows = 0
        for uri in on_main:
            with storage.open_text(self.warehouse, uri) as file:
                rows += sum(1 for _ in csv.DictReader(file))
        self.assertEqual(rows, summary["row_count_after_duplicates"], "main holds every generated row")

    def test_a_stopped_run_leaves_main_as_it_was(self) -> None:
        run_id = "manual__integration-stopped-run"
        main_before = self._files_on_main()
        lakefs.create_branch(RAW_URI, run_id, **self.keys)

        # What the 10 November run did before it was stopped: clear the zone, write one partial file.
        branch_uri = lakefs.on_branch(RAW_URI, run_id)
        storage.delete_prefix(self.warehouse, branch_uri)
        storage.put_text(self.warehouse, f"{branch_uri}/schema_version=v1/transaction_date=2026-01-01/transactions.csv",
                         "transaction_id\nhalf-written\n")

        self.assertEqual(self._files_on_main(), main_before, "nothing merged, so main is unchanged")

    def _files_on_main(self) -> list[str]:
        return storage.list_keys(self.warehouse, f"{RAW_URI}/", suffix=".csv")


def _two_days(output_dir: Path, raw_uri: str, labels_uri: str, warehouse: WarehouseConfig) -> OfflineGeneratorConfig:
    """A small run: day 1 in schema v1, day 2 in v2."""

    return OfflineGeneratorConfig(
        random_seed=7, n_transactions=200, n_customers=150, n_accounts=160, n_merchants=80,
        start_date=date(2026, 1, 1), days_history=2, currency="USD",
        skew_city="New York", skew_city_ratio=0.7, skew_merchant_category="online_marketplace",
        skew_merchant_category_ratio=0.6, duplicate_rate=0.02, schema_change_date=date(2026, 1, 2),
        burst_day_count=1, fraud_ring_count=1,
        output_dir=output_dir, raw_uri=raw_uri, labels_uri=labels_uri, warehouse=warehouse,
    )


def _wait_until_healthy(endpoint: str, seconds: int = 60) -> None:
    deadline = time.monotonic() + seconds
    while True:
        try:
            with urlopen(f"{endpoint}/api/v1/healthcheck", timeout=2):
                return
        except (URLError, ConnectionError):
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


def _api(endpoint: str, keys: dict[str, str], path: str, body: dict) -> None:
    login = base64.b64encode(f"{keys['lakefs_access_key']}:{keys['lakefs_secret_key']}".encode()).decode()
    request = Request(f"{endpoint}/api/v1{path}", data=json.dumps(body).encode(), method="POST",
                      headers={"Content-Type": "application/json", "Authorization": f"Basic {login}"})
    with urlopen(request, timeout=30):
        pass


if __name__ == "__main__":
    main()
