#!/usr/bin/env bash
# Checks Vault and everything that reads from it. Each section checks one kind of
# reader; with no section given, all of them run. Secret values are never printed,
# only whether they are set and whether two copies hash the same.
#   ./infra/vault/check.sh [section]...
#   ./infra/vault/check.sh server        Vault is up, unsealed, on Raft, over TLS, with an audit log
#   ./infra/vault/check.sh access        every secret is there, each reader reads its own and is refused the rest
#   ./infra/vault/check.sh cluster       every Kubernetes Secret is synced from Vault and holds the same value
#   ./infra/vault/check.sh cluster --sealed   and they all stay while Vault is sealed, and sync again once unsealed
# Needs VAULT_ADDR, VAULT_CACERT and a token from `vault login -method=userpass username=operator`.
set -euo pipefail
cd "$(dirname "$0")/../.."
kubectl() { command kubectl --context kind-fraudstream "$@"; }

failed=0
expect() {
  if [ "$3" = "$2" ]; then
    echo "  ok    $1: $3"
  else
    echo "  FAIL  $1: got '$3', wanted '$2'"
    failed=1
  fi
}

server() {
  echo "==> server"
  local status
  status=$(vault status -format=json 2>/dev/null || true)
  expect "initialized" true "$(jq -r '.initialized // empty' <<<"$status")"
  expect "unsealed" false "$(jq -r 'if .sealed == null then "" else .sealed end' <<<"$status")"
  expect "storage" raft "$(jq -r '.storage_type // empty' <<<"$status")"
  expect "KV mount secret/ at version" 2 \
    "$(vault secrets list -format=json 2>/dev/null | jq -r '."secret/".options.version // empty')"
  expect "audit device" file \
    "$(vault audit list -format=json 2>/dev/null | jq -r '."file/".type // empty')"
  expect "login for people" userpass \
    "$(vault auth list -format=json 2>/dev/null | jq -r '."userpass/".type // empty')"
  expect "health over HTTPS" 200 \
    "$(curl --cacert "$VAULT_CACERT" -s -o /dev/null -w '%{http_code}' "$VAULT_ADDR/v1/sys/health" || true)"
  expect "certificate issuer" "FraudStream Vault CA" \
    "$(openssl s_client -connect localhost:8200 -servername localhost </dev/null 2>/dev/null \
      | openssl x509 -noout -issuer -nameopt multiline 2>/dev/null | sed -n 's/^ *commonName *= *//p')"
}

# Every path in Vault, as `infra/vault/policies/` grants them.
PATHS=(platform/postgres platform/minio platform/pgvector platform/gateway platform/gateway-htpasswd
  ci/github compose/jenkins compose/airflow llm/benchmark llm/agents llm/risk-agent-b
  mcp/context-agent mcp/risk-agent mcp/mcp-check a2a/coordinator-agent a2a/a2a-check
  agents/substrate-rustfs cloud/postgres)

# Login and read through the API with curl: the token goes in a header from a file
# descriptor and the body from stdin, so neither shows in a command line.
login() { curl -s --cacert "$VAULT_CACERT" -X POST --data @- "$VAULT_ADDR/v1/auth/$1/login" | jq -r '.auth.client_token // empty'; }
read_status() {
  curl -s -o /dev/null -w '%{http_code}' --cacert "$VAULT_CACERT" \
    -H @<(printf 'X-Vault-Token: %s\n' "$1") "$VAULT_ADDR/v1/secret/data/$2"
}
approle_login() {
  local role_id secret_id
  role_id=$(vault read -field=role_id "auth/approle/role/$1/role-id" 2>/dev/null) || return 0
  secret_id=$(vault write -f -field=secret_id "auth/approle/role/$1/secret-id" 2>/dev/null) || return 0
  jq -n --arg r "$role_id" --arg s "$secret_id" '{role_id: $r, secret_id: $s}' | login approle
}
kubernetes_login() {
  jq -n --arg role "$1" --arg jwt "$(kubectl -n "$1" create token vault-secrets 2>/dev/null)" '{role: $role, jwt: $jwt}' \
    | login kubernetes
}
reads() {
  local who=$1 token=$2 path
  shift 2
  # A refusal only counts after a login: no token is refused too.
  expect "$who logs in" yes "${token:+yes}"
  [ -n "$token" ] || return 0
  for path in "$@"; do
    if [ "${path#!}" = "$path" ]; then
      expect "$who reads $path" 200 "$(read_status "$token" "$path")"
    else
      expect "$who refused ${path#!}" 403 "$(read_status "$token" "${path#!}")"
    fi
  done
}
versions() { for path in "${PATHS[@]}"; do vault kv metadata get -mount=secret -format=json "$path" 2>/dev/null | jq -r '.data.current_version'; done | tr '\n' ' '; }

access() {
  echo "==> access"
  local path token before
  for path in "${PATHS[@]}"; do
    expect "$path" set "$(vault kv metadata get -mount=secret "$path" >/dev/null 2>&1 && echo set || echo missing)"
  done
  token=$(approle_login jenkins)
  reads jenkins "$token" platform/postgres platform/minio platform/pgvector platform/gateway ci/github '!llm/agents'
  token=$(approle_login airflow)
  reads airflow "$token" platform/postgres platform/minio platform/pgvector platform/gateway '!ci/github'
  token=$(kubernetes_login fraudstream-apis)
  reads fraudstream-apis "$token" platform/postgres platform/pgvector platform/gateway-htpasswd '!platform/gateway'
  token=$(kubernetes_login observability)
  reads observability "$token" platform/gateway platform/gateway-htpasswd '!platform/postgres'
  before=$(versions)
  ./infra/vault/seed.sh >/dev/null
  expect "a second seed changes no version" "$before" "$(versions)"
}

# <namespace> <Secret> <key> <Vault path> <field>: each key of each synced Secret and where it comes from.
SYNCED=(
  "fraudstream-apis feature-store-registry POSTGRES_USER platform/postgres login"
  "fraudstream-apis feature-store-registry POSTGRES_PASSWORD platform/postgres password"
  "fraudstream-apis context-api POSTGRES_USER platform/postgres login"
  "fraudstream-apis context-api POSTGRES_PASSWORD platform/postgres password"
  "fraudstream-apis context-api PGVECTOR_USER platform/pgvector login"
  "fraudstream-apis context-api PGVECTOR_PASSWORD platform/pgvector password"
  "fraudstream-apis gateway-htpasswd htpasswd platform/gateway-htpasswd htpasswd"
  "observability gateway-htpasswd htpasswd platform/gateway-htpasswd htpasswd"
  "observability grafana-admin admin-user platform/gateway login"
  "observability grafana-admin admin-password platform/gateway password"
  "kubeflow gateway-htpasswd htpasswd platform/gateway-htpasswd htpasswd"
  "kubeflow lakehouse-credentials POSTGRES_USER platform/postgres login"
  "kubeflow lakehouse-credentials POSTGRES_PASSWORD platform/postgres password"
  "kubeflow lakehouse-credentials MINIO_ACCESS_KEY platform/minio login"
  "kubeflow lakehouse-credentials MINIO_SECRET_KEY platform/minio password"
)
# Scripts that may still make a Secret by hand: the GKE deploy, whose cluster can't
# reach Vault, and the keys' install scripts until they move to Vault too.
HAND_MADE_ALLOWED="cicd/deploy_agent.sh k8s/agents/install.sh k8s/agents/substrate.sh k8s/gateway/credentials.sh k8s/gke/deploy.sh k8s/llm/install.sh k8s/mcp/install.sh"

hash() { shasum -a 256 | cut -c1-16; }
secret_hash() { kubectl -n "$1" get secret "$2" -o jsonpath="{.data.$3}" 2>/dev/null | base64 -d | hash; }
synced_ready() { kubectl -n "$1" get externalsecret "$2" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null; }
all_ready() {
  local line
  for line in "${SYNCED[@]}"; do set -- $line; [ "$(synced_ready "$1" "$2")" = True ] || return 1; done
}

cluster() {
  echo "==> cluster"
  local line hashes=() i=0
  for line in "${SYNCED[@]}"; do
    set -- $line
    expect "$1/$2 synced" True "$(synced_ready "$1" "$2")"
    expect "$1/$2 $3 matches $4 $5" "$(vault kv get -mount=secret -field="$5" "$4" 2>/dev/null | hash)" "$(secret_hash "$1" "$2" "$3")"
  done
  local hand_made
  hand_made=$(comm -23 <(grep -rl "create secret" --include='*.sh' --include='*Jenkinsfile' k8s cicd Jenkinsfile | sort) \
    <(tr ' ' '\n' <<<"$HAND_MADE_ALLOWED" | sort) | xargs)
  expect "Secrets made by hand, outside the allowed scripts" none "${hand_made:-none}"
  [ "${1:-}" = --sealed ] || return 0

  echo "==> cluster, with Vault sealed"
  for line in "${SYNCED[@]}"; do set -- $line; hashes+=("$(secret_hash "$1" "$2" "$3")"); done
  VAULT_TOKEN=$(jq -r .root_token ~/.fraudstream/vault-init.json) vault operator seal >/dev/null
  echo "  sealed; waiting 2 minutes"
  sleep 120
  for line in "${SYNCED[@]}"; do
    set -- $line
    expect "$1/$2 $3 kept while sealed" "${hashes[$i]}" "$(secret_hash "$1" "$2" "$3")"
    i=$((i + 1))
  done
  expect "an ExternalSecret reports it can't sync" False "$(synced_ready observability grafana-admin)"
  ./infra/vault/unseal.sh >/dev/null
  echo "  unsealed; waiting up to 3 minutes for every Secret to sync again"
  for i in $(seq 36); do all_ready && break; sleep 5; done
  expect "every Secret synced again" yes "$(all_ready && echo yes || echo no)"
}

: "${VAULT_ADDR:?set VAULT_ADDR=https://localhost:8200}" "${VAULT_CACERT:?set VAULT_CACERT to infra/vault/tls/ca.crt}"
sections=("$@")
case " $* " in *" --sealed "*) sealed=--sealed ;; esac
[ ${#sections[@]} -gt 0 ] || sections=(server)
for section in "${sections[@]}"; do
  case "$section" in
    server) server ;;
    access) access ;;
    cluster) cluster "${sealed:-}" ;;
    --sealed) ;;
    *) echo "unknown section: $section" >&2; exit 2 ;;
  esac
done
exit "$failed"
