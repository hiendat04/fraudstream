#!/usr/bin/env bash
# Checks Vault and everything that reads from it. Each section checks one kind of
# reader; with no section given, all of them run. Secret values are never printed,
# only whether they are set and whether two copies hash the same.
#   ./infra/vault/check.sh [section]...
#   ./infra/vault/check.sh server        Vault is up, unsealed, on Raft, over TLS, with an audit log
#   ./infra/vault/check.sh access        every secret is there, each reader reads its own and is refused the rest
#   ./infra/vault/check.sh cluster       every Kubernetes Secret is synced from Vault and holds the same value
#   ./infra/vault/check.sh cluster --sealed   and they all stay while Vault is sealed, and sync again once unsealed
#   ./infra/vault/check.sh keys          the gateways' keys and their hashes come from Vault, made once
#   ./infra/vault/check.sh keys --rotate      and a rotated key reaches the A2A gateway: the old one is refused
#   ./infra/vault/check.sh compose       no secret in .env or the code, Compose needs Vault, Debezium reads its password
#   ./infra/vault/check.sh jenkins       Jenkins holds only its Vault login, and its credentials are read from Vault
#   ./infra/vault/check.sh airflow       Airflow reads its connections from Vault, and a task's password is masked
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
  reads jenkins "$token" platform/minio platform/gateway ci/github '!platform/postgres' '!llm/agents'
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
# The one script that may still make a Secret by hand: the GKE deploy, whose cluster
# can't reach Vault; it copies them from Vault.
HAND_MADE_ALLOWED="k8s/gke/deploy.sh"

hash() { shasum -a 256 | cut -c1-16; }
secret_hash() { kubectl -n "$1" get secret "$2" -o jsonpath="{.data.$3}" 2>/dev/null | base64 -d | hash; }
synced_ready() { kubectl -n "$1" get externalsecret "$2" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null; }
# Each line: <namespace> <Secret> <key> <Vault path> <field> [prefix]. The Secret is
# synced, and its value is the prefix and the field, compared by hash.
compare_synced() {
  local line
  for line in "$@"; do
    set -- $line
    expect "$1/$2 synced" True "$(synced_ready "$1" "$2")"
    expect "$1/$2 $3 matches $4 $5" \
      "$({ printf '%s' "${6:+$6 }"; vault kv get -mount=secret -field="$5" "$4" 2>/dev/null; } | hash)" \
      "$(secret_hash "$1" "$2" "$3")"
  done
}
all_ready() {
  local line
  for line in "${SYNCED[@]}"; do set -- $line; [ "$(synced_ready "$1" "$2")" = True ] || return 1; done
}

cluster() {
  echo "==> cluster"
  # set -- below replaces the arguments, so the flag is kept first.
  local mode=${1:-} line hashes=() i=0
  compare_synced "${SYNCED[@]}"
  local hand_made
  hand_made=$(comm -23 <(grep -rl "create secret" --include='*.sh' --include='*Jenkinsfile' k8s cicd Jenkinsfile | sort) \
    <(tr ' ' '\n' <<<"$HAND_MADE_ALLOWED" | sort) | xargs)
  expect "Secrets made by hand, outside the allowed scripts" none "${hand_made:-none}"
  [ "$mode" = --sealed ] || return 0

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

# The gateways' keys and their copies, as compare_synced reads them.
KEYS=(
  "llm llm-key-benchmark key llm/benchmark key"
  "llm llm-key-agents key llm/agents key"
  "llm llm-key-risk-agent-b key llm/risk-agent-b key"
  "mcp mcp-key-context-agent key mcp/context-agent key"
  "mcp mcp-key-risk-agent key mcp/risk-agent key"
  "mcp mcp-key-mcp-check key mcp/mcp-check key"
  "kagent a2a-key-coordinator-agent authorization a2a/coordinator-agent key Bearer"
  "kagent a2a-key-a2a-check authorization a2a/a2a-check key Bearer"
  "kagent kagent-llm-key key llm/agents key"
  "kagent kagent-llm-key-risk-agent-b key llm/risk-agent-b key"
  "kagent mcp-key-context-agent authorization mcp/context-agent key Bearer"
  "kagent mcp-key-risk-agent authorization mcp/risk-agent key Bearer"
  "kagent gateway-htpasswd htpasswd platform/gateway-htpasswd htpasswd"
  "agentregistry gateway-htpasswd htpasswd platform/gateway-htpasswd htpasswd"
  "ate-system substrate-rustfs accessKey agents/substrate-rustfs access_key"
  "ate-system substrate-rustfs secretKey agents/substrate-rustfs secret_key"
)
# <namespace> <Secret> <consumer> <Vault path>: each consumer's entry in a gateway's key list.
KEY_HASHES=(
  "llm llm-api-keys benchmark llm/benchmark"
  "llm llm-api-keys agents llm/agents"
  "llm llm-api-keys risk-agent-b llm/risk-agent-b"
  "mcp mcp-api-keys context-agent mcp/context-agent"
  "mcp mcp-api-keys risk-agent mcp/risk-agent"
  "mcp mcp-api-keys mcp-check mcp/mcp-check"
  "kagent a2a-api-keys coordinator-agent a2a/coordinator-agent"
  "kagent a2a-api-keys a2a-check a2a/a2a-check"
)
key_hash_of() { kubectl -n "$1" get secret "$2" -o json 2>/dev/null | jq -r --arg c "$3" '.data[$c] // empty | @base64d | fromjson | .keyHash'; }
vault_key_hash() { printf 'sha256:%s' "$(vault kv get -mount=secret -field=key "$1" 2>/dev/null | shasum -a 256 | cut -d' ' -f1)"; }
all_key_hashes() { local line; for line in "${KEY_HASHES[@]}"; do set -- $line; vault_key_hash "$4"; echo; done | hash; }
card_status() {
  curl -s -o /dev/null -w '%{http_code}' "localhost:18095/api/a2a-sandboxes/kagent/context-agent/.well-known/agent-card.json" \
    -H "x-kagent-host: kagent-controller.kagent" -H @<(printf 'Authorization: Bearer %s\n' "$1")
}

keys() {
  echo "==> keys"
  # set -- below replaces the arguments, so the flag is kept first.
  local mode=${1:-} line before forward old new
  compare_synced "${KEYS[@]}"
  for line in "${KEY_HASHES[@]}"; do
    set -- $line
    expect "$1/$2 $3 is the hash of $4" "$(vault_key_hash "$4")" "$(key_hash_of "$1" "$2" "$3")"
  done
  reads llm "$(kubernetes_login llm)" llm/agents '!mcp/mcp-check'
  reads mcp "$(kubernetes_login mcp)" mcp/mcp-check '!llm/agents'
  reads kagent "$(kubernetes_login kagent)" a2a/a2a-check llm/agents mcp/risk-agent '!llm/benchmark'
  before=$(all_key_hashes)
  for line in "${KEY_HASHES[@]}"; do set -- $line; ./infra/vault/new-key.sh "$4" >/dev/null; done
  expect "making the keys again changes none" "$before" "$(all_key_hashes)"
  [ "$mode" = --rotate ] || return 0

  echo "==> keys, rotating a2a-check"
  command kubectl --context kind-fraudstream -n kagent port-forward svc/a2a-gateway 18095:8080 >/dev/null &
  forward=$!
  sleep 2
  old=$(vault kv get -mount=secret -field=key a2a/a2a-check)
  expect "the current key is let in" 200 "$(card_status "$old")"
  ./infra/vault/rotate-key.sh a2a/a2a-check >/dev/null
  new=$(vault kv get -mount=secret -field=key a2a/a2a-check)
  echo "  rotated; waiting up to 3 minutes for the gateway to take the new key"
  for _ in $(seq 36); do [ "$(card_status "$new")" = 200 ] && break; sleep 5; done
  expect "the new key is let in" 200 "$(card_status "$new")"
  expect "the old key is refused" 401 "$(card_status "$old")"
  expect "a2a-api-keys holds the new hash" "$(vault_key_hash a2a/a2a-check)" "$(key_hash_of kagent a2a-api-keys a2a-check)"
  kill "$forward" 2>/dev/null; wait "$forward" 2>/dev/null || true
}

# Variables that hold a secret; .env must leave each one empty.
SECRET_VARIABLES="POSTGRES_USER POSTGRES_PASSWORD MINIO_ACCESS_KEY MINIO_SECRET_KEY MINIO_ROOT_USER MINIO_ROOT_PASSWORD
  PGVECTOR_USER PGVECTOR_PASSWORD GATEWAY_USER GATEWAY_PASSWORD GITHUB_USER GITHUB_TOKEN JENKINS_ADMIN_PASSWORD
  AIRFLOW_FERNET_KEY AIRFLOW_JWT_SECRET AIRFLOW_API_SECRET_KEY CLOUD_POSTGRES_PASSWORD"

compose() {
  echo "==> compose"
  local name in_env old_defaults
  in_env=$(for name in $SECRET_VARIABLES; do ! grep -q "^$name=." .env 2>/dev/null || printf '%s ' "$name"; done | xargs)
  expect "secrets with a value in .env" none "${in_env:-none}"
  old_defaults=$(git grep -l "fraudstream_local_password\|llm_local_password\|change-me" -- ':!docs' ':!infra/vault/check.sh' | xargs || true)
  expect "files holding an old default password" none "${old_defaults:-none}"
  expect "Compose refuses to start without Vault" refused \
    "$(env -i PATH="$PATH" HOME="$HOME" docker compose config --quiet 2>/dev/null && echo started || echo refused)"
  expect "Debezium reads config from the environment" env \
    "$(docker inspect fraudstream-debezium-connect --format '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null \
      | sed -n 's/^CONNECT_CONFIG_PROVIDERS=//p')"
  expect "the Debezium connector" RUNNING \
    "$(curl -s localhost:18083/connectors/fraudstream-bronze-raw-transactions/status | jq -r '.connector.state // empty')"
}

JENKINS_CREDENTIALS="minio github gateway"

# The audit log's reads of one path by one AppRole in the last 30 minutes, from
# the given containers when named (the access check logs in as each AppRole too).
audited_reads() {
  local from
  from=$(for container in "${@:3}"; do
    docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' "$container" 2>/dev/null
  done | xargs)
  docker logs fraudstream-vault --since 30m 2>/dev/null | jq -Rr --arg path "$1" --arg role "$2" --arg from "$from" '
    fromjson? | select(.type == "response" and .request.path == $path and .auth.metadata.role_name == $role)
    | .request.remote_address as $ip | select($from == "" or ($from | split(" ") | index($ip)) != null) | .time' | wc -l | xargs
}

jenkins() {
  echo "==> jenkins"
  local admin jar credentials id crumb fetched reads
  expect "Jenkins' secret files" "JENKINS_ADMIN_PASSWORD VAULT_ROLE_ID VAULT_SECRET_ID" \
    "$(docker exec fraudstream-jenkins ls /run/secrets 2>/dev/null | sort | xargs)"
  admin=$(vault kv get -mount=secret -field=admin_password compose/jenkins)
  jar=$(mktemp)
  # The login reaches curl through its config on stdin, never its command line; the cookie keeps the crumb's session.
  jenkins_api() { printf 'user = "admin:%s"\n' "$admin" | curl -s -K - -b "$jar" -c "$jar" "$@"; }
  # While Jenkins starts, it answers with an HTML page instead of JSON.
  credentials=$(jenkins_api "localhost:18086/credentials/store/system/domain/_/api/json?depth=1" \
    | jq -c '[.credentials[]? | {id, typeName}]' 2>/dev/null || echo null)
  expect "Jenkins' API answers" yes "$([ "$credentials" != null ] && echo yes || echo "no: still starting?")"
  for id in $JENKINS_CREDENTIALS; do
    expect "credential $id is read from Vault" yes \
      "$(jq -r --arg id "$id" 'any(.[]; .id == $id and (.typeName | test("Vault")))' <<<"$credentials" | sed 's/true/yes/;s/false/no/')"
  done
  expect "Jenkins logs in to Vault" yes "$(jq -r 'any(.[]; .id == "vault-approle")' <<<"$credentials" | sed 's/true/yes/;s/false/no/')"
  # Only deploy builds use the gateway login, so Jenkins is asked to fetch it here, as a build would.
  crumb=$(jenkins_api localhost:18086/crumbIssuer/api/json | jq -r '"\(.crumbRequestField): \(.crumb)"' 2>/dev/null || true)
  fetched=$(jenkins_api -X POST localhost:18086/scriptText -H "$crumb" --data-urlencode 'script=
def c = com.cloudbees.plugins.credentials.SystemCredentialsProvider.instance.credentials.find { it.id == "gateway" }
println(c != null && c.password.plainText ? "resolved" : "missing")' || true)
  rm -f "$jar"
  expect "Jenkins fetches the gateway login" resolved "$fetched"
  reads=$(audited_reads secret/data/platform/gateway jenkins fraudstream-jenkins)
  expect "the audit log shows Jenkins reading the gateway login" yes "$([ "${reads:-0}" -gt 0 ] && echo yes || echo "no reads in the last 30 minutes")"
}

airflow() {
  echo "==> airflow"
  local connection passwords reads admin run rendered
  connection=$(docker exec fraudstream-airflow-scheduler airflow connections get postgres -o json 2>/dev/null \
    | jq -r '.[0] // . | "\(.conn_type) \(.host) password \(if (.password // "") != "" then "set" else "missing" end)"' || true)
  expect "the postgres connection, from Vault" "postgres postgres password set" "$connection"
  passwords=$(docker exec fraudstream-airflow-scheduler env 2>/dev/null \
    | grep -oE '^(POSTGRES|PGVECTOR|GATEWAY)_PASSWORD|^MINIO_SECRET_KEY' | xargs || true)
  expect "no password in the scheduler's environment" none "${passwords:-none}"
  reads=$(audited_reads secret/data/platform/postgres airflow fraudstream-airflow-scheduler fraudstream-airflow-api-server)
  expect "the audit log shows Airflow reading the postgres login" yes "$([ "${reads:-0}" -gt 0 ] && echo yes || echo "no reads in the last 30 minutes")"
  # The last drift run's first task: its rendered environment must show *** for the password.
  # Every user is an admin here, so the token endpoint hands one out without a login.
  admin=$(curl -s localhost:18081/auth/token | jq -r '.access_token // empty' 2>/dev/null || true)
  [ -n "$admin" ] || admin=$(curl -s -X POST localhost:18081/auth/token -H 'Content-Type: application/json' -d '{}' \
    | jq -r '.access_token // empty' 2>/dev/null || true)
  expect "Airflow's API answers" yes "${admin:+yes}"
  run=$(curl -s -H "Authorization: Bearer $admin" "localhost:18081/api/v2/dags/fraudstream_drift_monitoring/dagRuns?order_by=-start_date&limit=1" \
    | jq -r '.dag_runs[0].dag_run_id // empty' 2>/dev/null || true)
  rendered=$(curl -s -H "Authorization: Bearer $admin" \
    "localhost:18081/api/v2/dags/fraudstream_drift_monitoring/dagRuns/$run/taskInstances" \
    | jq -r '[.task_instances[] | select(.rendered_fields.env != null)][0].rendered_fields.env.GATEWAY_PASSWORD // empty' 2>/dev/null || true)
  expect "a task's rendered GATEWAY_PASSWORD" "***" "${rendered:-no drift run with a rendered env yet}"
}

# Not named ansible, which would hide the command.
ansible_lookups() {
  echo "==> ansible"
  local output found
  # The playbook's own variables, read as a run reads them, on this computer only. On macOS
  # a forked worker dies looking up the system proxy, so no_proxy skips that.
  output=$(cd infra/ansible && no_proxy='*' ansible -i localhost, localhost -c local -m ansible.builtin.debug \
    -e @group_vars/stores.yml -e @roles/seed/defaults/main.yml \
    -a '{"msg": "{{ \"set\" if postgres_password else \"missing\" }} {{ \"set\" if seed_local_postgres_user else \"missing\" }}"}' 2>&1 || true)
  found=$(grep -q SUCCESS <<<"$output" && sed -n 's/^ *"msg": "\([^"]*\)".*/\1/p' <<<"$output" || true)
  expect "the cloud's postgres password and the seed's login, from Vault" "set set" \
    "${found:-$(grep -o 'FAILED.*' <<<"$output" | head -1 | cut -c1-200)}"
  expect "the playbook's syntax" passes \
    "$(cd infra/ansible && ansible-playbook -i localhost, site.yml --syntax-check >/dev/null 2>&1 && echo passes || echo fails)"
}

: "${VAULT_ADDR:?set VAULT_ADDR=https://localhost:8200}" "${VAULT_CACERT:?set VAULT_CACERT to infra/vault/tls/ca.crt}"
sections=("$@")
case " $* " in *" --sealed "*) sealed=--sealed ;; esac
case " $* " in *" --rotate "*) rotate=--rotate ;; esac
[ ${#sections[@]} -gt 0 ] || sections=(server)
for section in "${sections[@]}"; do
  case "$section" in
    server) server ;;
    access) access ;;
    cluster) cluster "${sealed:-}" ;;
    keys) keys "${rotate:-}" ;;
    compose) compose ;;
    jenkins) jenkins ;;
    airflow) airflow ;;
    ansible) ansible_lookups ;;
    --sealed | --rotate) ;;
    *) echo "unknown section: $section" >&2; exit 2 ;;
  esac
done
exit "$failed"
