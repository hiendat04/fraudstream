#!/usr/bin/env bash
# Copy what Airflow runs into a deploy folder. Jenkins does this on every deploy;
# run it once by hand so the folder is filled before Airflow first starts.
#   ./ci/sync_airflow.sh /your/airflow/deploy/folder
set -euo pipefail

target=$1
cd "$(dirname "$0")/.."

# --delete would erase a checkout's uncommitted work.
[ ! -d "$target/.git" ] || { echo "$target is a git checkout, not a deploy folder" >&2; exit 1; }

for dir in airflow/dags airflow/include airflow/config airflow/scripts src configs feature_store/src feature_store/feature_repo; do
  mkdir -p "$target/$dir"
  rsync -a --delete --exclude __pycache__ "$dir/" "$target/$dir/"
done
