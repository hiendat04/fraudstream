# Sourced by every script that talks to a cluster: which cluster, and the three
# names that differ between local dev (kind) and GCP (gke). POSIX sh, so the
# Jenkinsfile's sh steps can source it too.
#   ./k8s/llm/check.sh
#   FRAUDSTREAM_CLUSTER=gke FRAUDSTREAM_DOMAIN=<gateway ip>.sslip.io \
#   FRAUDSTREAM_IMAGE_REGISTRY=<repository url> ./k8s/llm/check.sh
FRAUDSTREAM_CLUSTER=${FRAUDSTREAM_CLUSTER:-kind}
case "$FRAUDSTREAM_CLUSTER" in
  kind)
    CONTEXT=kind-fraudstream
    DOMAIN=fraudstream.localhost
    IMAGE_REGISTRY=
    ;;
  gke)
    CONTEXT=fraudstream-gke
    DOMAIN=${FRAUDSTREAM_DOMAIN:?FRAUDSTREAM_DOMAIN is not set: the gateway address as <ip>.sslip.io}
    IMAGE_REGISTRY=${FRAUDSTREAM_IMAGE_REGISTRY:?FRAUDSTREAM_IMAGE_REGISTRY is not set: the Artifact Registry repository URL}
    ;;
  *)
    echo "FRAUDSTREAM_CLUSTER must be kind or gke, not $FRAUDSTREAM_CLUSTER" >&2
    return 1 2>/dev/null || exit 1
    ;;
esac
export FRAUDSTREAM_CLUSTER CONTEXT DOMAIN IMAGE_REGISTRY

# A manifest with the gateway names written for the laptop, for this cluster.
render() { sed "s/fraudstream\.localhost/$DOMAIN/g" "$1"; }

# An image repository on this cluster: the local name on kind, the registry's on gke.
image() { echo "${IMAGE_REGISTRY:+$IMAGE_REGISTRY/}$1"; }
