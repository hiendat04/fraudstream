# Sourced by scripts that call an API through the gateway. gateway_curl is curl
# that trusts the local CA and connects to NGINX whatever the host name, so the
# certificate is checked against the real host.
#   GATEWAY_ADDRESS  where NGINX listens: the kind node from Jenkins, 127.0.0.1 from local computer
#   GATEWAY_CA       the local CA certificate; read from the cluster when unset
gateway_address=${GATEWAY_ADDRESS:-fraudstream-control-plane}
gateway_ca=${GATEWAY_CA:-}
if [ -z "$gateway_ca" ]; then
  gateway_ca=$(mktemp)
  kubectl -n cert-manager get secret local-ca -o jsonpath='{.data.ca\.crt}' | base64 -d > "$gateway_ca"
fi

gateway_curl() {
  curl --connect-to "::$gateway_address:" --cacert "$gateway_ca" "$@"
}
