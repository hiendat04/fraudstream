"""On GKE, pods reach the platform VM's stores by the names they use on the laptop."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from unittest import TestCase, main

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = "k8s/gke/vm_services.sh"
NAMES = "postgres|pgvector|redis|minio|mlflow|vault|kafka|memgraph|trino|lakefs"
# A host name in a URL, host:port, NAME_HOST: name, or NAME=name.
HOST = re.compile(rf"(//({NAMES})[:/\"'\s]|\b({NAMES}):\d|_HOST[:=]\s*\"?({NAMES})\b|nc -z -w3 ({NAMES})\b)")
# Files that name a VM store but carry no namespace: where their pods run.
RUNS_IN = {
    "k8s/apis/context-api.yaml": "fraudstream-apis",
    "k8s/apis/drift-detection.yaml": "fraudstream-apis",
    "k8s/apis/inference-api.yaml": "fraudstream-apis",
    "k8s/Dockerfile.ml": "kubeflow",
    "k8s/Dockerfile.serving": "kserve-models",
    "k8s/bootstrap.sh": "default",
}


def run_raw(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["bash", SCRIPT, *args], cwd=ROOT, capture_output=True, text=True)


def docs_of(*args: str) -> list[dict]:
    result = run_raw(*args)
    result.check_returncode()
    return [json.loads(doc) for doc in result.stdout.split("---") if doc.strip()]


def find(docs: list[dict], kind: str, name: str, namespace: str) -> dict:
    return next(d for d in docs if d["kind"] == kind and d["metadata"] == {**d["metadata"], "name": name, "namespace": namespace})


def namespaces_naming_vm_stores() -> dict[str, str]:
    """Each namespace whose pods name a VM store, with one file that shows it."""

    files = subprocess.run(["git", "ls-files", "k8s"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.split()
    used = {}
    for name in files:
        if name == SCRIPT or not (name.endswith((".yaml", ".yml")) or name in RUNS_IN):
            continue
        for doc in (ROOT / name).read_text().split("\n---"):
            if not HOST.search(doc):
                continue
            if "kind: ClusterSecretStore" in doc:
                namespace = "external-secrets"  # External Secrets' controller is what calls Vault.
            elif match := re.search(r"^\s+namespace:\s*(\S+)", doc, re.MULTILINE):
                namespace = match[1]
            else:
                namespace = RUNS_IN.get(name, f"unknown: add {name} to RUNS_IN")
            used.setdefault(namespace, name)
    return used


class VmServicesTest(TestCase):
    def test_pgvector_reaches_its_own_host_port(self) -> None:
        docs = docs_of("10.10.0.5", "fraudstream-apis")
        service = find(docs, "Service", "pgvector", "fraudstream-apis")
        endpoints = find(docs, "EndpointSlice", "pgvector", "fraudstream-apis")
        self.assertEqual(service["spec"]["ports"][0]["port"], 5432)
        self.assertNotIn("selector", service["spec"])
        self.assertEqual(endpoints["metadata"]["labels"]["kubernetes.io/service-name"], "pgvector")
        self.assertEqual(endpoints["endpoints"][0]["addresses"], ["10.10.0.5"])
        self.assertEqual(endpoints["ports"][0]["port"], 15432)

    def test_kafka_advertised_address_is_mapped(self) -> None:
        docs = docs_of("10.10.0.5", "kubeflow")
        self.assertEqual(find(docs, "Service", "kafka", "kubeflow")["spec"]["ports"][0]["port"], 29092)

    def test_every_namespace_that_names_a_vm_service_is_listed(self) -> None:
        listed = {d["metadata"]["namespace"] for d in docs_of("10.10.0.5")}
        missing = {ns: f for ns, f in namespaces_naming_vm_stores().items() if ns not in listed}
        self.assertEqual(missing, {})

    def test_no_vm_ip_means_no_output(self) -> None:
        result = run_raw()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    main()
