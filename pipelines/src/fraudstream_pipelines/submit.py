"""Compile the pipeline and run it against a Kubeflow Pipelines endpoint.

Reach the endpoint first with:
    kubectl port-forward -n kubeflow svc/ml-pipeline-ui 8080:80

Then:
    uv run python -m fraudstream_pipelines.submit --num-nodes 2

CI registers a version instead of starting a run:
    uv run python -m fraudstream_pipelines.submit --host http://localhost:8888 --upload-version 9f789a7
"""

import argparse
import sys
from pathlib import Path

from kfp import Client, compiler

from fraudstream_pipelines.pipeline import fraud_training_pipeline

EXPERIMENT = "fraud-model-training"
PIPELINE = "fraud-training"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="http://localhost:8080")
    parser.add_argument("--num-nodes", type=int, default=2, help="how many training workers")
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument("--end-date", default="2026-06-30")
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--compile-only", action="store_true")
    parser.add_argument("--output", default="fraud_training_pipeline.yaml")
    parser.add_argument("--upload-version", help="register the pipeline under this version name instead of running it")
    arguments = parser.parse_args()

    target = Path(arguments.output)
    compiler.Compiler().compile(fraud_training_pipeline, str(target))
    print(f"compiled to {target}")
    if arguments.compile_only:
        return 0

    client = Client(host=arguments.host)
    if arguments.upload_version:
        pipeline_id = client.get_pipeline_id(PIPELINE)
        if pipeline_id is None:
            pipeline_id = client.upload_pipeline(str(target), pipeline_name=PIPELINE).pipeline_id
        version = client.upload_pipeline_version(
            str(target), pipeline_version_name=arguments.upload_version, pipeline_id=pipeline_id
        )
        print(f"{PIPELINE}: version {arguments.upload_version} ({version.pipeline_version_id})")
        return 0

    run = client.create_run_from_pipeline_package(
        str(target),
        arguments={
            "num_nodes": arguments.num_nodes,
            "start_date": arguments.start_date,
            "end_date": arguments.end_date,
        },
        experiment_name=EXPERIMENT,
        enable_caching=False,
    )
    print(f"run {run.run_id}")
    print(f"{arguments.host}/#/runs/details/{run.run_id}")

    result = client.wait_for_run_completion(run.run_id, timeout=arguments.timeout)
    state = str(result.state).upper()
    print(f"finished: {state}")
    return 0 if state == "SUCCEEDED" else 1


if __name__ == "__main__":
    sys.exit(main())
