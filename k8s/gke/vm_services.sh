#!/usr/bin/env bash
# Print a Service and an EndpointSlice per VM store and namespace, so GKE pods
# reach the platform VM by the names they use on kind (postgres, minio, vault...).
# A Service without a selector sends its port to whatever the EndpointSlice says:
# the VM's internal IP and the host port Compose publishes there.
#   k8s/gke/vm_services.sh VM_IP [NAMESPACE...] | kubectl apply -f -
set -euo pipefail
vm_ip=${1:?usage: $0 VM_IP [NAMESPACE...]}
shift
# The namespaces whose pods name a VM store. vault is read only by External Secrets'
# controller, and default is where bootstrap.sh probes the lakehouse.
NAMESPACES=(default external-secrets fraudstream-apis kserve-models kubeflow)
[ $# -gt 0 ] || set -- "${NAMESPACES[@]}"

# name, port pods use, port published on the VM (docker-compose.cloud.yml).
stores=$(cat <<'EOF'
postgres 5432 5432  
pgvector 5432 15432
redis 6379 6379
minio 9000 9000
mlflow 5000 5000
vault 8200 8200
kafka 29092 29092
memgraph 7687 7687
trino 8080 18082
lakefs 8000 18000
EOF
)

for namespace in "$@"; do
  while read -r name port host_port; do
    cat <<EOF
---
{"apiVersion": "v1", "kind": "Service",
 "metadata": {"name": "$name", "namespace": "$namespace"},
 "spec": {"ports": [{"port": $port, "protocol": "TCP"}]}}
---
{"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice",
 "metadata": {"name": "$name", "namespace": "$namespace", "labels": {"kubernetes.io/service-name": "$name"}},
 "addressType": "IPv4",
 "endpoints": [{"addresses": ["$vm_ip"]}],
 "ports": [{"port": $host_port, "protocol": "TCP"}]}
EOF
  done <<<"$stores"
done
