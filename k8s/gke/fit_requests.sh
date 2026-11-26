#!/usr/bin/env bash
# GKE only: lower the CPU each platform pod reserves to 20m, and keep its limit.
# The charts' defaults reserve about ten times what the pods use (kubectl top), and the
# scheduler places pods by what they reserve, so 4 vCPU fill up while idle.
# Left alone: workloads another controller owns, since it would put its own numbers back,
# workloads Helm installed, since its next upgrade refuses a field kubectl changed,
# and workloads an autoscaler scales by CPU percentage, which a lower request would inflate.
# An install resets its own requests, so this runs again after the installers.
#   ./k8s/gke/fit_requests.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
. k8s/cluster.sh
kubectl() { command kubectl --context "$CONTEXT" "$@"; }
CPU=20

scaled=$(kubectl get hpa -A -o json | jq -c '[.items[]
  | "\(.metadata.namespace) \(.spec.scaleTargetRef.kind | ascii_downcase)/\(.spec.scaleTargetRef.name)"]')
kubectl get deployments,statefulsets,daemonsets -A -o json | jq -r --argjson cpu "$CPU" --argjson scaled "$scaled" '
  def millicores: if endswith("m") then rtrimstr("m") | tonumber else tonumber * 1000 end;
  .items[]
  | select((.metadata.namespace | test("^(kube-|gmp-|gke-)")) | not)
  | select(.metadata.ownerReferences == null)
  | select(.metadata.annotations["meta.helm.sh/release-name"] == null)
  | select([.spec.template.spec.containers[].resources.requests.cpu // "0" | millicores] | max > $cpu)
  | "\(.metadata.namespace) \(.kind | ascii_downcase)/\(.metadata.name)"
  | select(. as $workload | $scaled | index([$workload]) | not)' |
while read -r namespace workload; do
  kubectl -n "$namespace" set resources "$workload" --requests="cpu=${CPU}m" >/dev/null
  echo "  ${CPU}m  $namespace $workload"
done
