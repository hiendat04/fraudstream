#!/usr/bin/env bash
# Runs one guidellm goodput search against the LLM gateway from inside the cluster,
# prints guidellm's summary and saves the full report. The workload and the SLA are
# fixed in bench/job.yaml so every run is comparable. A fixed concurrency can replace
# the search when it would stop before reaching what the experiment changes.
#   ./k8s/llm/bench.sh <label> [model] [prefixes] [profile]
#   ./k8s/llm/bench.sh 0-baseline
#   ./k8s/llm/bench.sh 2-two-slots qwen3-4b 1 kind=concurrent,streams=2
#   ./k8s/llm/bench.sh 5-routing-pool qwen3-1.7b 4
set -euo pipefail
cd "$(dirname "$0")/../.."
source k8s/versions.sh
label=${1:?usage: bench.sh <label> [model] [prefixes] [profile]}
model=${2:-qwen3-4b}
prefixes=${3:-1}
profile=${4:-kind=goodput,target_attainment=0.95,initial_streams=1}
job="bench-$(printf %s "$label" | tr -c 'a-z0-9' '-' | cut -c1-50)"
out="k8s/llm/bench/results/$label.json"

kubectl -n llm delete job "$job" --ignore-not-found >/dev/null
sed -e "s#__JOB__#$job#" -e "s#__MODEL__#$model#" -e "s#__PREFIXES__#$prefixes#" \
  -e "s#__PROFILE__#$profile#" -e "s#__IMAGE__#$GUIDELLM_IMAGE#" k8s/llm/bench/job.yaml | kubectl apply -f - >/dev/null
echo "==> $job: $model, $prefixes shared prefix(es), $profile; takes 15-40 minutes"
# Wait for either outcome: waiting only for Complete would sit out the whole
# timeout when the Job fails.
until kubectl -n llm get job "$job" -o jsonpath='{.status.conditions[?(@.status=="True")].type}' \
  | grep -qE 'Complete|Failed'; do sleep 15; done
if ! kubectl -n llm get job "$job" -o jsonpath='{.status.conditions[?(@.status=="True")].type}' | grep -q Complete; then
  kubectl -n llm logs "job/$job" --tail=60; exit 1
fi
kubectl -n llm logs "job/$job" > "$out.log"
sed -n '/^===REPORT===$/q;p' "$out.log" | tail -60
sed -n '/^===REPORT===$/,$p' "$out.log" | tail -n +2 > "$out"
rm "$out.log"
echo "saved $out ($(du -h "$out" | cut -f1))"
