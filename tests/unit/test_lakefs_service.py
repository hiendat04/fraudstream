"""The lakeFS service: pinned, its objects in MinIO, and every key from Vault."""

from __future__ import annotations

import re
import unittest
from pathlib import Path


def _service(compose: str, name: str) -> str:
    match = re.search(rf"^  {name}:\n(.*?)(?=^  \S|^\S)", compose, flags=re.MULTILINE | re.DOTALL)
    assert match, f"no {name} service"
    return match.group(1)


class LakeFSServiceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.compose = Path("docker-compose.yml").read_text(encoding="utf-8")
        cls.lakefs = _service(cls.compose, "lakefs")

    def test_the_image_is_pinned(self) -> None:
        self.assertRegex(self.lakefs, r"image: treeverse/lakefs:\d+\.\d+\.\d+\n")

    def test_its_objects_go_to_minio_in_their_own_bucket(self) -> None:
        self.assertIn("LAKEFS_BLOCKSTORE_S3_ENDPOINT: http://minio:9000", self.lakefs)
        self.assertIn("mc mb --ignore-existing local/lakefs", _service(self.compose, "minio-bucket-init"))

    def test_every_key_comes_from_the_environment(self) -> None:
        for name in (
            "LAKEFS_AUTH_ENCRYPT_SECRET_KEY",
            "LAKEFS_BLOCKSTORE_S3_CREDENTIALS_ACCESS_KEY_ID",
            "LAKEFS_BLOCKSTORE_S3_CREDENTIALS_SECRET_ACCESS_KEY",
            "LAKEFS_INSTALLATION_ACCESS_KEY_ID",
            "LAKEFS_INSTALLATION_SECRET_ACCESS_KEY",
        ):
            with self.subTest(name=name):
                self.assertRegex(self.lakefs, rf'{name}: "\$\{{\w+:\?run \. infra/vault/env\.sh')

    def test_env_sh_exports_the_keys_compose_reads(self) -> None:
        env_sh = Path("infra/vault/env.sh").read_text(encoding="utf-8")
        for name in ("LAKEFS_ACCESS_KEY", "LAKEFS_SECRET_KEY", "LAKEFS_ENCRYPT_KEY"):
            with self.subTest(name=name):
                self.assertRegex(env_sh, rf"(?m)^{name}=\$\(_vault_field ", "read from Vault")
                self.assertRegex(env_sh, rf"(?m)^export .*\b{name}\b|^  .*\b{name}\b")


if __name__ == "__main__":
    unittest.main()
