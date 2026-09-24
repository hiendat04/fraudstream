#!/usr/bin/env bash
# Run one project's test suite, or all of them.
#   ./ci/test.sh            every suite
#   ./ci/test.sh api ml     just those
set -euo pipefail
cd "$(dirname "$0")/.."

run_root()          { PYTHONPATH=src uv run python -m unittest discover -s tests/unit -p 'test_*.py'; }
run_ml()            { cd ml && PYTHONPATH=src uv run python -m unittest discover -s tests; }
run_pipelines()     { cd pipelines && PYTHONPATH=src uv run python -m unittest discover -s tests; }
run_feature_store() { cd feature_store && PYTHONPATH=src:../src uv run python -m unittest discover -s tests; }
run_datahub()       { cd datahub && PYTHONPATH=src uv run python -m unittest discover -s tests; }
run_api()           { cd api && uv run --all-extras --group reference pytest -q --cov; }
run_serving()       { cd serving && uv run pytest -q --cov; }

all_suites=(root ml pipelines feature_store datahub api serving)
suites=("${@:-${all_suites[@]}}")

for suite in "${suites[@]}"; do
  echo "--- $suite"
  ( "run_${suite}" )
done
