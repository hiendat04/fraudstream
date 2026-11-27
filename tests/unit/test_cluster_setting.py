"""The one cluster setting: k8s/cluster.sh, sourced by every script that talks to a cluster."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from unittest import TestCase, main, skipUnless

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


MODELS = "{ cat k8s/llm/models/qwen3-4b.yaml; echo ---; cat k8s/llm/models/qwen3-1-7b.yaml; }"
PATCH = "with_gke_requests k8s/gke/requests/llm-models.yaml"


@skipUnless(shutil.which("kubectl"), "needs kubectl, for kustomize")
class GkeRequestsTest(TestCase):
    """GKE's smaller CPU requests come with each install; kind's installs are untouched."""

    def run_with_path(self, command: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        return sourced(command, {**env, "PATH": os.environ["PATH"]})

    def test_kind_takes_every_manifest_and_chart_as_it_is(self) -> None:
        manifest = (ROOT / "k8s/llm/models/qwen3-4b.yaml").read_text()
        out = self.run_with_path(f"{PATCH} < k8s/llm/models/qwen3-4b.yaml; on_gke --values extra.yaml", {})
        self.assertEqual(out.stdout, manifest)

    def test_gke_lowers_the_cpu_request_and_keeps_the_rest(self) -> None:
        out = self.run_with_path(f"{MODELS} | {PATCH}", GKE)
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertEqual(out.stdout.count("cpu: 100m"), 2)
        self.assertNotIn("cpu: 500m", out.stdout)
        self.assertIn("ghcr.io/ggml-org/llama.cpp", out.stdout)
        self.assertEqual(self.run_with_path("on_gke --values extra.yaml", GKE).stdout.strip(), "--values extra.yaml")

    def test_a_patch_for_a_workload_the_manifest_lacks_fails(self) -> None:
        # A renamed upstream workload stops the install instead of keeping its default.
        out = self.run_with_path(f"{PATCH} < k8s/llm/models/qwen3-4b.yaml", GKE)
        self.assertNotEqual(out.returncode, 0)

    def test_every_gke_requests_file_is_used_by_an_install(self) -> None:
        scripts = "".join(path.read_text() for path in (ROOT / "k8s").rglob("*.sh"))
        unused = [path.name for path in (ROOT / "k8s/gke/requests").iterdir() if f"gke/requests/{path.name}" not in scripts]
        self.assertEqual(unused, [])

    def test_no_script_names_the_kind_cluster_itself(self) -> None:
        hits = subprocess.run(
            ["git", "grep", "-n", "-E", "kind-fraudstream|kind load", "--", "*.sh", "Jenkinsfile",
             *(f":!{path}" for path in ALLOWED)],
            cwd=ROOT, capture_output=True, text=True,
        ).stdout
        self.assertEqual(hits, "")


if __name__ == "__main__":
    main()
