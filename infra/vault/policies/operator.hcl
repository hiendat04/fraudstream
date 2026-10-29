path "secret/*" {
  capabilities = ["create", "read", "update", "patch", "delete", "list"]
}

path "sys/*" {
  capabilities = ["read", "list"]
}

path "sys/audit" {
  capabilities = ["read", "list", "sudo"]
}

path "auth/approle/role/+/role-id" {
  capabilities = ["read"]
}

path "auth/approle/role/+/secret-id" {
  capabilities = ["update"]
}
