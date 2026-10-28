#!/usr/bin/env bash
# Checks the LLM gateway end to end: keys, the model allowlist, the request limit
# and PII masking, from a curl pod inside the cluster, and that the gateway counted
# the mask.
#   ./k8s/llm/check.sh
# The split of fraud-detection-llm between versions has its own check: ./k8s/llm/check-split.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
kubectl config use-context kind-fraudstream >/dev/null

kubectl -n llm wait --for=condition=Programmed gateway/llm-gateway --timeout=60s >/dev/null
kubectl -n llm rollout status deploy/qwen3-4b --timeout=60s >/dev/null

proxy=$(kubectl -n llm get pods -l gateway.networking.k8s.io/gateway-name=llm-gateway \
  -o jsonpath='{.items[0].metadata.name}')
masks() {
  kubectl get --raw "/api/v1/namespaces/llm/pods/$proxy:15020/proxy/metrics" \
    | awk '/^agentgateway_guardrail_checks_total/ && /action="[Mm]ask"/ {n += $NF} END {print n + 0}'
}
before=$(masks)

kubectl -n llm create configmap llm-check --from-file=check.sh=k8s/llm/check-requests.sh \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n llm delete pod llm-check --ignore-not-found >/dev/null
cat <<'EOF' | kubectl apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: llm-check
  namespace: llm
spec:
  restartPolicy: Never
  containers:
    - name: check
      image: curlimages/curl:8.21.0
      command: ["sh", "/check/check.sh"]
      env:
        - name: AGENTS_KEY
          valueFrom: {secretKeyRef: {name: llm-key-agents, key: key}}
        - name: BENCHMARK_KEY
          valueFrom: {secretKeyRef: {name: llm-key-benchmark, key: key}}
      volumeMounts:
        - {name: check, mountPath: /check}
  volumes:
    - name: check
      configMap: {name: llm-check}
EOF
for i in $(seq 120); do
  phase=$(kubectl -n llm get pod llm-check -o jsonpath='{.status.phase}')
  [ "$phase" = Succeeded ] || [ "$phase" = Failed ] && break
  sleep 2
done
kubectl -n llm logs llm-check
after=$(masks)
if [ "$after" -gt "$before" ]; then echo "ok   the gateway masked PII ($before -> $after)"
else echo "FAIL the gateway counted no mask ($before -> $after)"; phase=Failed; fi
kubectl -n llm delete pod llm-check >/dev/null
# Every dashboard panel must have data. The requests above produced some; Prometheus
# scrapes every 15 seconds.
sleep 30
prom=/api/v1/namespaces/observability/services/kps-prometheus:9090/proxy/api/v1/query
python3 - "$prom" <<'PY' || phase=Failed
import json, re, subprocess, sys, urllib.parse
text = open("k8s/observability/dashboards/llm.yaml").read()
board = json.loads(text.split("llm.json: |", 1)[1].replace("\n    ", "\n"))
bad = 0
for panel in board["panels"]:
    for target in panel["targets"]:
        url = sys.argv[1] + "?query=" + urllib.parse.quote(target["expr"])
        out = json.loads(subprocess.run(["kubectl", "get", "--raw", url], capture_output=True, text=True, check=True).stdout)
        found = len(out["data"]["result"])
        print(("ok  " if found else "FAIL") + f" panel '{panel['title']}' {target['refId']}: {found} series")
        bad += not found
sys.exit(1 if bad else 0)
PY
[ "$phase" = Succeeded ]
