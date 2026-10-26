#!/usr/bin/env bash
# Trigger one Airflow DAG run and wait for it; exit 0 only if it succeeds. Jenkins runs this after a
# deploy, and it works the same by hand. Airflow runs in Docker Compose as fraudstream-airflow-scheduler.
#   ./cicd/run_dag.sh fraudstream_rag_index [timeout_seconds]
set -euo pipefail

dag=$1
timeout=${2:-1800}
run_id="ci-$(date -u +%Y%m%dT%H%M%S)"
airflow() { docker exec fraudstream-airflow-scheduler airflow "$@"; }

# A paused DAG keeps a triggered run queued forever.
airflow dags unpause "$dag" > /dev/null
airflow dags trigger "$dag" --run-id "$run_id" > /dev/null
echo "triggered $dag run $run_id"

state=queued
waited=0
while [[ $state != success && $state != failed ]] && (( waited < timeout )); do
  sleep 15
  waited=$((waited + 15))
  state=$(airflow dags state "$dag" "$run_id" | tail -1)
done
airflow tasks states-for-dag-run "$dag" "$run_id" || true
echo "$dag $run_id: $state after ${waited}s"
[[ $state == success ]]
