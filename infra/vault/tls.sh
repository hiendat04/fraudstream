#!/usr/bin/env bash
# Makes Vault's own certificate authority and its server certificate, into
# infra/vault/tls/. Vault starts before the cluster and its cert-manager, so it
# can't use the cluster's local CA. Does nothing when the certificate exists.
#   ./infra/vault/tls.sh
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p tls
cd tls
# Python 3.13 and later refuse a CA without key usage.
CA_USAGE="keyUsage=critical,keyCertSign,cRLSign"

# A CA made without it is re-issued from the same key and name, in place, so the
# server certificate and every copy of the old CA keep working.
if [ -f ca.crt ] && ! openssl x509 -in ca.crt -noout -ext keyUsage 2>/dev/null | grep -q "Certificate Sign"; then
  openssl req -x509 -new -key ca.key -days 3650 -subj "/CN=FraudStream Vault CA" -addext "$CA_USAGE" -out ca.crt 2>/dev/null
  echo "re-issued: infra/vault/tls/ca.crt, with key usage"
fi
[ ! -f vault.crt ] || { echo "kept: infra/vault/tls/vault.crt exists"; exit 0; }

umask 077

# Create the CA private key and certificate
openssl req -x509 -newkey rsa:4096 -nodes -days 3650 -subj "/CN=FraudStream Vault CA" -addext "$CA_USAGE" \
  -keyout ca.key -out ca.crt 2>/dev/null

# Create the Vault server private key and CSR
openssl req -newkey rsa:2048 -nodes -subj "/CN=vault" -keyout vault.key -out vault.csr 2>/dev/null

# Sign the server certificate using your CA
openssl x509 -req -in vault.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 730 -out vault.crt \
  -extfile <(printf 'subjectAltName=DNS:vault,DNS:fraudstream-vault,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n') 2>/dev/null

rm -f vault.csr ca.srl
chmod 644 ca.crt vault.crt
echo "made: infra/vault/tls/ca.crt and vault.crt"
