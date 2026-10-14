"""Package a registered model into a folder that the serving image copies in.

The model ships inside its image, so a pod starts with it on disk: nothing to
download at start, no MLflow to reach, and rolling back means running the previous
image. The folder holds the MLflow model and a card with what the predictor would
otherwise ask the registry for: the version and the decision threshold.

    cd serving && MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD=false PYTHONPATH=src uv run python -m \
        fraudstream_serving.fetch models:/fraud-detection/4 /tmp/model --tracking-uri http://localhost:18085
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

CARD_FILE = "model_card.json"


def fetch(model_uri: str, out_dir: str | Path, tracking_uri: str | None = None) -> dict:
    """Download one registered model version and write its card. `latest` and aliases resolve here."""

    if not model_uri.startswith("models:/"):
        raise ValueError(f"only registered models (models:/...) can be packaged, not {model_uri}")

    from mlflow import MlflowClient
    from mlflow.store.artifact.artifact_repository_registry import get_artifact_repository
    from mlflow.store.artifact.utils.models import get_model_name_and_version

    client = MlflowClient(tracking_uri)
    name, version = get_model_name_and_version(client, model_uri)
    run = client.get_run(client.get_model_version(name, version).run_id)

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    # Not download_artifacts("models:/..."): MLflow resolves that through its process-wide
    # registry, not this client, and in a fresh process reads a new local mlflow.db.
    get_artifact_repository(
        client.get_model_version_download_uri(name, version), tracking_uri=tracking_uri
    ).download_artifacts("", dst_path=str(out))
    card = {
        "name": name,
        "version": str(version),
        "run_id": run.info.run_id,
        "threshold": run.data.metrics.get("threshold"),
        "metrics": dict(run.data.metrics),
        "data_snapshot_id": run.data.params.get("data_snapshot_id"),
    }
    (out / CARD_FILE).write_text(json.dumps(card, indent=2, sort_keys=True))
    return card


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("model_uri", help="models:/fraud-detection/<version|latest> or @alias")
    parser.add_argument("out_dir")
    parser.add_argument("--tracking-uri", default=None)
    arguments = parser.parse_args()
    card = fetch(arguments.model_uri, arguments.out_dir, arguments.tracking_uri)
    print(json.dumps({key: card[key] for key in ("name", "version", "threshold")}))


if __name__ == "__main__":
    main()
