#!/usr/bin/env bash
# Prints the agent fleet in one table: the workers ready and wanted, the actors
# kagent's controller knows (each session, and each golden snapshot) by status,
# how long each kind of start takes, and the workers' memory against a pod per
# session. The starts: a pod (the newest coordinator pod, creation to Ready), a
# golden snapshot (each agent's newest ActorTemplate, creation to Ready), a worker
# (the newest worker pod), and a warm session (the node agent restoring an actor
# from its golden snapshot onto a running worker: how many and their average over
# the last hour, and the atenet router's part, choosing the worker). Reads through the API
# server, so no port-forwards. With --html, writes the table as a page instead.
#   ./k8s/agents/fleet.sh
#   ./k8s/agents/fleet.sh --html fleet.html
set -euo pipefail
# --context on every call, not use-context: another script may be writing the kubeconfig at the same time.
kubectl() { command kubectl --context kind-fraudstream "$@"; }

case "${1:-}" in
  "") out= ;;
  --html) out=${2:?usage: $0 [--html file]} ;;
  *) echo "usage: $0 [--html file]" >&2; exit 2 ;;
esac

# What one agent pod used in doc 33, in MiB.
pod_low=7 pod_high=15
prom=/api/v1/namespaces/observability/services/kps-prometheus:9090/proxy/api/v1/query
promql() { kubectl get --raw "$prom?query=$(jq -rn --arg q "$1" '$q|@uri')" | jq -r '.data.result[0].value[1] // "nan"'; }
# Creation to Ready, in seconds, of the newest item of a list on stdin.
started() { jq -r '.items | max_by(.metadata.creationTimestamp) | [(.status.conditions[] | select(.type == "Ready") | .lastTransitionTime | fromdate) - (.metadata.creationTimestamp | fromdate)] | .[0]'; }
section() {
  if [ -z "$out" ]; then echo "==> $1"; else printf '<tr><th colspan="2">%s</th></tr>\n' "$1"; fi
}
# A label and its value; in the page the value takes its tone's colour: ok, warn, or idle (also any nan).
row() {
  if [ -z "$out" ]; then printf '  %-44s %s\n' "$1" "$2"; return; fi
  local tone=${3:-}
  [[ $2 == nan* ]] && tone=idle
  jq -rn --arg label "$1" --arg value "$2" --arg tone "$tone" \
    '"<tr><td>\($label | @html)</td><td class=\"\($tone)\">\($value | @html)</td></tr>"'
}

if [ -n "$out" ]; then
  exec 3>&1 >"$out.part"
  cat <<EOF
<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Agent fleet</title>
<style>
:root { --bg: #f8fafc; --card: #fff; --text: #0f172a; --muted: #64748b; --line: #e2e8f0; --head: #1d4ed8; --ok: #15803d; --warn: #b45309; }
@media (prefers-color-scheme: dark) { :root { --bg: #0b1120; --card: #111827; --text: #e5e7eb; --muted: #94a3b8; --line: #1f2937; --head: #60a5fa; --ok: #4ade80; --warn: #fbbf24; } }
body { margin: 0; padding: 32px 16px; background: var(--bg); color: var(--text); font: 15px/1.5 system-ui, sans-serif; }
main { max-width: 720px; margin: 0 auto; }
h1 { font-size: 22px; margin: 0 0 4px; }
p { margin: 0 0 24px; color: var(--muted); }
table { width: 100%; border-collapse: separate; border-spacing: 0; background: var(--card); border: 1px solid var(--line); border-radius: 8px; }
th { text-align: left; padding: 16px 16px 6px; color: var(--head); font-size: 12px; text-transform: uppercase; letter-spacing: .06em; border-top: 1px solid var(--line); }
tr:first-child th { border-top: 0; }
td { padding: 8px 16px; border-top: 1px solid var(--line); }
td:first-child { white-space: pre; }
td:last-child { text-align: right; font-variant-numeric: tabular-nums; font-weight: 600; white-space: nowrap; }
.ok { color: var(--ok); } .warn { color: var(--warn); } .idle { color: var(--muted); }
</style></head><body><main>
<h1>Agent fleet</h1>
<p>kind-fraudstream · $(date '+%Y-%m-%d %H:%M')</p>
<table>
EOF
fi

status=$(kubectl get --raw /api/v1/namespaces/kagent/services/kagent-controller:8083/proxy/api/substrate/status)
sessions=$(jq '[.data.actors[] | select(.atespace != "ate-golden")] | length' <<<"$status")

section workers
read -r ready wanted < <(kubectl -n kagent get deploy kagent-default-deployment -o json | jq -r '"\(.status.readyReplicas // 0) \(.spec.replicas)"')
row "ready / wanted" "$ready / $wanted" "$([ "$ready" = "$wanted" ] && echo ok || echo warn)"

section actors
jq -r '.data.actors | group_by([.atespace == "ate-golden", .status])[] |
  "\(if .[0].atespace == "ate-golden" then "golden snapshots" else "sessions" end)\t\(.[0].status)\t\(length)"' <<<"$status" |
  while IFS=$'\t' read -r what state count; do
    case $state in Running|Ready) tone=ok ;; Suspended) tone=idle ;; *) tone=warn ;; esac
    row "$what, $state" "$count" "$tone"
  done

section starts
row "pod (newest coordinator pod)" "$(kubectl -n kagent get pods -l app.kubernetes.io/name=coordinator-agent -o json | started) s"
for agent in context-agent risk-agent; do
  row "golden snapshot ($agent)" "$(kubectl -n kagent get actortemplates -l "kagent.dev/sandbox-agent=$agent" -o json | started) s"
done
row "worker (newest worker pod)" "$(kubectl -n kagent get pods -l ate.dev/worker-pool=kagent-default -o json | started) s"
# Averages, not percentiles: nearly every restore lands in one histogram bucket, and a percentile inside it is only interpolation.
restore='rpc_server_call_duration_seconds_%s{rpc_method="atelet.AteomHerder/Restore"}'
row "warm sessions started" "$(promql "sum(increase($(printf "$restore" count)[1h]))" | xargs printf '%.0f')"
row "warm session, average" "$(promql "sum(increase($(printf "$restore" sum)[1h])) / sum(increase($(printf "$restore" count)[1h]))" |
  xargs printf '%.2f') s" ok
row "  of it routing, average" "$(promql 'sum(increase(atenet_router_route_duration_seconds_sum{outcome="ok"}[1h])) / sum(increase(atenet_router_route_duration_seconds_count{outcome="ok"}[1h])) * 1000' |
  xargs printf '%.0f') ms"

section memory
used=$(kubectl -n kagent top pods -l ate.dev/worker-pool=kagent-default --no-headers | awk '{sum += $3} END {print sum}')
row "the workers, for $sessions sessions" "$used MiB" "$( ((used < sessions * pod_low)) && echo ok)"
row "a pod per session, at $pod_low-$pod_high MiB each" "$((sessions * pod_low))-$((sessions * pod_high)) MiB" idle

if [ -n "$out" ]; then
  echo '</table></main></body></html>'
  exec >&3
  mv "$out.part" "$out"
  echo "wrote $out"
fi
