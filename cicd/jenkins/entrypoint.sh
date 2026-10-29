#!/usr/bin/env bash
# Trusts Vault's CA, then starts Jenkins as the image would. Jenkins reads its
# credentials from Vault over HTTPS, and Vault's certificate comes from a CA of
# its own, so the JVM gets a truststore with the default CAs plus that one.
set -euo pipefail
truststore=/var/jenkins_home/cacerts
cp "$JAVA_HOME/lib/security/cacerts" "$truststore"
keytool -importcert -noprompt -keystore "$truststore" -storepass changeit \
  -alias fraudstream-vault -file /run/vault-ca.crt >/dev/null
exec /usr/bin/tini -- /usr/local/bin/jenkins.sh "$@"
