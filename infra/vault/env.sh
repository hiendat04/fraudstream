# Sourced, not run: exports the stores', the gateway's, CI's and Airflow's secrets
# from Vault into the current shell, for Docker Compose and for jobs run by hand.
# Needs Vault unsealed and `vault login -method=userpass username=operator`.
#   . infra/vault/env.sh
#   PYTHONPATH=src python -m fraudstream.jobs.silver.transactions
_vault_dir=$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)
: "${VAULT_ADDR:=https://localhost:8200}" "${VAULT_CACERT:=$_vault_dir/tls/ca.crt}"
export VAULT_ADDR VAULT_CACERT
vault token lookup >/dev/null 2>&1 || {
  echo "vault is sealed or you are not logged in: ./infra/vault/unseal.sh, then vault login -method=userpass username=operator" >&2
  return 1 2>/dev/null || exit 1
}

# Assigned first and exported after, so a failed read stops the script.
_vault_field() { vault kv get -mount=secret -field="$2" "$1"; }
POSTGRES_USER=$(_vault_field platform/postgres login)
POSTGRES_PASSWORD=$(_vault_field platform/postgres password)
MINIO_ROOT_USER=$(_vault_field platform/minio login)
MINIO_ROOT_PASSWORD=$(_vault_field platform/minio password)
PGVECTOR_USER=$(_vault_field platform/pgvector login)
PGVECTOR_PASSWORD=$(_vault_field platform/pgvector password)
GATEWAY_USER=$(_vault_field platform/gateway login)
GATEWAY_PASSWORD=$(_vault_field platform/gateway password)
GITHUB_USER=$(_vault_field ci/github username)
GITHUB_TOKEN=$(_vault_field ci/github token)
JENKINS_ADMIN_PASSWORD=$(_vault_field compose/jenkins admin_password)
AIRFLOW_FERNET_KEY=$(_vault_field compose/airflow fernet_key)
AIRFLOW_JWT_SECRET=$(_vault_field compose/airflow jwt_secret)
AIRFLOW_API_SECRET_KEY=$(_vault_field compose/airflow api_secret_key)
MINIO_ACCESS_KEY=$MINIO_ROOT_USER
MINIO_SECRET_KEY=$MINIO_ROOT_PASSWORD

# Jenkins' own Vault login, a fresh secret id each run: Jenkins reads every other
# credential from Vault with it when a build uses one.
VAULT_ROLE_ID=$(vault read -field=role_id auth/approle/role/jenkins/role-id)
VAULT_SECRET_ID=$(vault write -f -field=secret_id auth/approle/role/jenkins/secret-id)

# Airflow's, the same way: its secrets backend reads the connections with it.
AIRFLOW_VAULT_ROLE_ID=$(vault read -field=role_id auth/approle/role/airflow/role-id)
AIRFLOW_VAULT_SECRET_ID=$(vault write -f -field=secret_id auth/approle/role/airflow/secret-id)
export POSTGRES_USER POSTGRES_PASSWORD MINIO_ROOT_USER MINIO_ROOT_PASSWORD MINIO_ACCESS_KEY MINIO_SECRET_KEY \
  PGVECTOR_USER PGVECTOR_PASSWORD GATEWAY_USER GATEWAY_PASSWORD GITHUB_USER GITHUB_TOKEN JENKINS_ADMIN_PASSWORD \
  AIRFLOW_FERNET_KEY AIRFLOW_JWT_SECRET AIRFLOW_API_SECRET_KEY VAULT_ROLE_ID VAULT_SECRET_ID AIRFLOW_VAULT_ROLE_ID AIRFLOW_VAULT_SECRET_ID

unset -f _vault_field
unset _vault_dir
