#!/usr/bin/env bash
# Copies every secret in use today into Vault, once: the stores' and CI's from
# .env, the gateway login and the LLM, MCP and A2A keys from the cluster's Secrets.
# A path that exists is kept as it is (check-and-set 0), so running it again
# changes nothing. On a new machine, where neither has them, the passwords and keys
# are made; only the GitHub token can't be, and is reported, and the script exits 1.
# Runs as the operator; never prints a value.
#   ./infra/vault/seed.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
: "${VAULT_ADDR:=https://localhost:8200}" "${VAULT_CACERT:=$PWD/infra/vault/tls/ca.crt}"
: "${VAULT_TOKEN:=$(cat ~/.vault-token)}"
export VAULT_ADDR VAULT_CACERT VAULT_TOKEN
kubectl() { command kubectl --context kind-fraudstream "$@"; }
[ ! -f .env ] || { set -a; . ./.env; set +a; }
random_key() { openssl rand -hex 24; }

missing=0
secret() { kubectl -n "$1" get secret "$2" -o jsonpath="{.data.$3}" 2>/dev/null | base64 -d; }

# put <path> <json object of fields>: written only if the path is new, with the
# body from stdin and the token from a file descriptor, never in a command line.
put() {
  if vault kv metadata get -mount=secret "$1" >/dev/null 2>&1; then
    echo "  kept  $1"
  elif jq -e 'all(.[]; . != null and . != "")' >/dev/null <<<"$2"; then
    jq '{options: {cas: 0}, data: .}' <<<"$2" | curl -sf --cacert "$VAULT_CACERT" \
      -H @<(printf 'X-Vault-Token: %s\n' "$VAULT_TOKEN") -X POST --data @- "$VAULT_ADDR/v1/secret/data/$1" >/dev/null
    echo "  made  $1"
  else
    echo "  MISSING  $1: a field is empty; put it in through the Vault UI"
    missing=1
  fi
}

echo "==> the stores, the gateway and CI"
# A new machine's stores start with whatever is made here.
: "${POSTGRES_DB:=fraudstream}" "${POSTGRES_USER:=fraudstream}" "${POSTGRES_PASSWORD:=$(random_key)}"
: "${MINIO_ACCESS_KEY:=fraudstream}" "${MINIO_SECRET_KEY:=$(random_key)}"
: "${PGVECTOR_USER:=llm}" "${PGVECTOR_PASSWORD:=$(random_key)}" "${GATEWAY_USER:=fraudstream}"
: "${JENKINS_ADMIN_PASSWORD:=$(random_key)}" "${CLOUD_POSTGRES_PASSWORD:=$(random_key)}"
export POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD MINIO_ACCESS_KEY MINIO_SECRET_KEY PGVECTOR_USER \
  PGVECTOR_PASSWORD GATEWAY_USER JENKINS_ADMIN_PASSWORD CLOUD_POSTGRES_PASSWORD
# Values reach jq through its environment (env.NAME), never as arguments.
put platform/postgres "$(jq -n '{conn_type: "postgres", host: "postgres", port: "5432",
  schema: env.POSTGRES_DB, login: env.POSTGRES_USER, password: env.POSTGRES_PASSWORD}')"
put platform/minio "$(jq -n '{conn_type: "aws", login: env.MINIO_ACCESS_KEY, password: env.MINIO_SECRET_KEY}')"
put platform/pgvector "$(jq -n '{conn_type: "postgres", host: "pgvector", port: "5432", schema: "llm",
  login: env.PGVECTOR_USER, password: env.PGVECTOR_PASSWORD}')"
# .env may leave it empty; the cluster's Grafana login is the gateway's.
GATEWAY_PASSWORD=${GATEWAY_PASSWORD:-$(secret observability grafana-admin admin-password)}
GATEWAY_PASSWORD=${GATEWAY_PASSWORD:-$(random_key)}
HTPASSWD="$GATEWAY_USER:$(printf %s "$GATEWAY_PASSWORD" | openssl passwd -apr1 -stdin)"
export GATEWAY_PASSWORD HTPASSWD
put platform/gateway "$(jq -n '{conn_type: "http", login: env.GATEWAY_USER, password: env.GATEWAY_PASSWORD}')"
put platform/gateway-htpasswd "$(jq -n '{htpasswd: env.HTPASSWD}')"
put ci/github "$(jq -n '{username: env.GITHUB_USER, token: env.GITHUB_TOKEN}')"
put compose/jenkins "$(jq -n '{admin_password: env.JENKINS_ADMIN_PASSWORD}')"
: "${AIRFLOW_FERNET_KEY:=$(python3 -c 'import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())')}"
: "${AIRFLOW_JWT_SECRET:=$(openssl rand -hex 32)}" "${AIRFLOW_API_SECRET_KEY:=$(openssl rand -hex 32)}"
export AIRFLOW_FERNET_KEY AIRFLOW_JWT_SECRET AIRFLOW_API_SECRET_KEY
put compose/airflow "$(jq -n '{fernet_key: env.AIRFLOW_FERNET_KEY, jwt_secret: env.AIRFLOW_JWT_SECRET,
  api_secret_key: env.AIRFLOW_API_SECRET_KEY}')"
put cloud/postgres "$(jq -n '{password: env.CLOUD_POSTGRES_PASSWORD}')"

echo "==> the LLM, MCP and A2A keys"
# key_from <namespace> <secret> <field>: the cluster's key, or a new one.
key_from() { KEY=$(secret "$1" "$2" "$3"); KEY=${KEY#Bearer }; KEY=${KEY:-$(random_key)}; export KEY; }
for consumer in benchmark agents risk-agent-b; do
  key_from llm "llm-key-$consumer" key
  put "llm/$consumer" "$(jq -n '{key: env.KEY}')"
done
for consumer in context-agent risk-agent mcp-check; do
  key_from mcp "mcp-key-$consumer" key
  put "mcp/$consumer" "$(jq -n '{key: env.KEY}')"
done
for consumer in coordinator-agent a2a-check; do
  key_from kagent "a2a-key-$consumer" authorization
  put "a2a/$consumer" "$(jq -n '{key: env.KEY}')"
done
ACCESS_KEY=$(secret ate-system substrate-rustfs accessKey); ACCESS_KEY=${ACCESS_KEY:-$(openssl rand -hex 12)}
SECRET_KEY=$(secret ate-system substrate-rustfs secretKey); SECRET_KEY=${SECRET_KEY:-$(random_key)}
export ACCESS_KEY SECRET_KEY
put agents/substrate-rustfs "$(jq -n '{access_key: env.ACCESS_KEY, secret_key: env.SECRET_KEY}')"

exit "$missing"
