path "secret/*" {
  capabilities = ["create", "read", "update", "patch", "delete", "list"]
}

path "sys/*" {
  capabilities = ["read", "list"]
}

path "sys/audit" {
  capabilities = ["read", "list", "sudo"]
}
