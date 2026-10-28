#!/usr/bin/env bash
# Judges the LLM split or the risk agent's from one window of Prometheus: a row per
# variant, then pass, fail or inconclusive with the reasons.
#   ./k8s/agents/ab-testing-report.sh llm [30m]
#   ./k8s/agents/ab-testing-report.sh agent 2h
# Exits 0 on pass, 10 on fail and 12 when the candidate has too few calls to say.
set -euo pipefail
cd "$(dirname "$0")/../.."

command kubectl --context kind-fraudstream -n observability port-forward svc/kps-prometheus 19090:9090 >/dev/null &
forward=$!
trap 'kill $forward 2>/dev/null; wait $forward 2>/dev/null || true' EXIT
sleep 2

PYTHONPATH=agents/src env -u VIRTUAL_ENV uv run --project agents python agents/tools/ab_testing_report.py "$1" \
  --prometheus http://127.0.0.1:19090 --window "${2:-30m}"
