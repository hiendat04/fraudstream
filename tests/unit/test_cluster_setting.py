"""The one cluster setting: k8s/cluster.sh, sourced by every script that talks to a cluster."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest import TestCase, main

ROOT = Path(__file__).resolve().parents[2]
GKE = {
    "FRAUDSTREAM_CLUSTER": "gke",
    "FRAUDSTREAM_DOMAIN": "34.1.2.3.sslip.io",
    "FRAUDSTREAM_IMAGE_REGISTRY": "us-central1-docker.pkg.dev/project/fraudstream",
}
# The two files allowed to name the kind cluster: the setting itself, and the
# image step's kind branch. The Jenkins kubeconfig and the session's return to
# the laptop name it on purpose until the cloud Jenkins replaces them.
ALLOWED = ("k8s/cluster.sh", "cicd/ship_image.sh", "cicd/jenkins/kubeconfig.sh", "infra/session.sh")


def sourced(command: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Run COMMAND in sh after sourcing the setting, with only ENV set."""

    return subprocess.run(
        ["sh", "-c", f". k8s/cluster.sh && {command}"],
        cwd=ROOT, env={"PATH": "/usr/bin:/bin", **env}, capture_output=True, text=True,
    )


class ClusterSettingTest(TestCase):
    def test_kind_is_the_default(self) -> None:
        result = sourced('echo "$CONTEXT|$DOMAIN|$IMAGE_REGISTRY"', {})
        self.assertEqual(result.stdout.strip(), "kind-fraudstream|fraudstream.localhost|")

    def test_gke_names_its_own_context_domain_and_registry(self) -> None:
        result = sourced('echo "$CONTEXT|$DOMAIN|$(image fraudstream-inference)"', GKE)
        self.assertEqual(
            result.stdout.strip(),
            "fraudstream-gke|34.1.2.3.sslip.io|us-central1-docker.pkg.dev/project/fraudstream/fraudstream-inference",
        )

    def test_kind_images_keep_their_local_name(self) -> None:
        self.assertEqual(sourced("image fraudstream-inference", {}).stdout.strip(), "fraudstream-inference")

    def test_gke_without_a_domain_stops_with_a_reason(self) -> None:
        result = sourced("echo reached", {"FRAUDSTREAM_CLUSTER": "gke", "FRAUDSTREAM_IMAGE_REGISTRY": "r"})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("FRAUDSTREAM_DOMAIN", result.stderr)
        self.assertNotIn("reached", result.stdout)

    def test_an_unknown_cluster_is_refused(self) -> None:
        result = sourced("echo reached", {"FRAUDSTREAM_CLUSTER": "minikube"})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("kind or gke", result.stderr)

    def test_render_swaps_every_gateway_name(self) -> None:
        out = sourced("render k8s/observability/ingress.yaml", GKE).stdout
        self.assertNotIn("fraudstream.localhost", out)
        self.assertIn("grafana.34.1.2.3.sslip.io", out)

    def test_no_script_names_the_kind_cluster_itself(self) -> None:
        hits = subprocess.run(
            ["git", "grep", "-n", "-E", "kind-fraudstream|kind load", "--", "*.sh", "Jenkinsfile",
             *(f":!{path}" for path in ALLOWED)],
            cwd=ROOT, capture_output=True, text=True,
        ).stdout
        self.assertEqual(hits, "")


if __name__ == "__main__":
    main()
