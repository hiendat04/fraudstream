#!/usr/bin/env bash
# Make an image built on local available to the cluster: loaded into the
# kind node, or pushed to Artifact Registry with the VM's own service account.
#   ./cicd/ship_image.sh fraudstream-inference:9f789a7
set -euo pipefail
cd "$(dirname "$0")/.."
. k8s/cluster.sh
built=$1

if [ "$FRAUDSTREAM_CLUSTER" = kind ]; then
  kind load docker-image "$built" --name fraudstream
  exit 0
fi

# A name already in the registry is pushed as it is.
remote=$(image "${built#"$IMAGE_REGISTRY/"}")
[ "$remote" = "$built" ] || docker tag "$built" "$remote"
# The token goes from the metadata server straight into docker login's stdin.
curl -sf -H 'Metadata-Flavor: Google' \
  http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token \
  | sed -E 's/.*"access_token":"([^"]+)".*/\1/' \
  | docker login -u oauth2accesstoken --password-stdin "https://${IMAGE_REGISTRY%%/*}" >/dev/null
docker push "$remote"
