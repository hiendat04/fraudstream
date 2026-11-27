"""The platform VM's Compose override: nothing on the VM listens on every address."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main, skipUnless

ROOT = Path(__file__).resolve().parents[2]
VM_IP = "10.10.0.5"


@skipUnless(shutil.which("docker"), "needs the docker CLI")
class CloudComposeTest(TestCase):
    def merged(self) -> dict:
        """The merged stack as the VM starts it, with placeholder secrets."""

        with TemporaryDirectory() as folder:
            env = Path(folder, "env")
            # Every variable the files require, with a placeholder value.
            names = set()
            for name in ("docker-compose.yml", "infra/vault/compose.yml", "docker-compose.cloud.yml"):
                text = (ROOT / name).read_text()
                names |= {part.split(":?")[0] for part in text.split("${")[1:] if ":?" in part.split("}")[0]}
            values = {name: "placeholder" for name in names}
            values |= {"FRAUDSTREAM_VM_IP": VM_IP, "FRAUDSTREAM_DOMAIN": "34.1.2.3.sslip.io"}
            env.write_text("".join(f"{name}={value}\n" for name, value in values.items()))
            result = subprocess.run(
                ["docker", "compose", "--env-file", str(env), "-f", "docker-compose.yml", "-f", "docker-compose.cloud.yml",
                 "--profile", "orchestration", "--profile", "feature-store", "--profile", "ci", "config", "--format", "json"],
                cwd=ROOT, capture_output=True, text=True, check=True, env={**os.environ, "COMPOSE_PROFILES": ""},
            )
        return json.loads(result.stdout)

    def test_every_port_binds_to_the_internal_ip_or_loopback(self) -> None:
        services = self.merged()["services"]
        open_ports = [
            f"{name}:{port['published']}" for name, service in services.items() for port in service.get("ports", [])
            if port.get("host_ip") not in (VM_IP, "127.0.0.1")
        ]
        self.assertEqual(open_ports, [])
        self.assertEqual({p["host_ip"] for p in services["vault"]["ports"]}, {VM_IP})

    def test_jenkins_has_only_the_default_network(self) -> None:
        self.assertEqual(list(self.merged()["services"]["jenkins"]["networks"]), ["default"])


if __name__ == "__main__":
    main()
