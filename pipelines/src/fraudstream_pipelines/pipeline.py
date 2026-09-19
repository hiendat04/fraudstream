"""The training pipeline graph: the notebook's stages, wired together."""

from kfp import dsl, kubernetes

from fraudstream_pipelines.components import (
    HANDOFF_BUCKET,
    HANDOFF_PREFIX,
    TRAINING_IMAGE,
    evaluate,
    prepare,
    retrieve,
    register_model,
    save_bundle,
    split,
    train_baseline,
    train_distributed,
)

FEAST_REPO = "/opt/fraudstream/feature_store/feature_repo"
MINIO_ENDPOINT = "minio:9000"
MLFLOW_URI = "http://mlflow:5000"
DATA_TABLE = "iceberg.ml.training_data"
CREDENTIALS_SECRET = "lakehouse-credentials"

LAKEHOUSE_KEYS = {
    "POSTGRES_USER": "POSTGRES_USER",
    "POSTGRES_PASSWORD": "POSTGRES_PASSWORD",
    "MINIO_ACCESS_KEY": "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY": "MINIO_SECRET_KEY",
}


def _with_lakehouse_credentials(task):
    """Hand a step the usernames and passwords it needs to reach the lakehouse.

    Only the steps that actually read storage get these. The rest work on
    pipeline artifacts alone and never see a credential.
    """

    return kubernetes.use_secret_as_env(
        task, secret_name=CREDENTIALS_SECRET, secret_key_to_env=LAKEHOUSE_KEYS
    )


@dsl.pipeline(
    name="fraud-model-training",
    description="Retrieve features, split by time, train on several workers, evaluate once.",
)
def fraud_training_pipeline(
    start_date: str = "2026-01-01",
    end_date: str = "2026-06-30",
    train_end: str = "2026-05-05",
    validation_end: str = "2026-05-30",
    num_nodes: int = 2,
    max_depth: int = 3,
    learning_rate: float = 0.1,
    num_boost_round: int = 400,
    feast_repo: str = FEAST_REPO,
    minio_endpoint: str = MINIO_ENDPOINT,
    training_image: str = TRAINING_IMAGE,
    mlflow_uri: str = MLFLOW_URI,
    data_table: str = DATA_TABLE,
) -> None:
    """Train the fraud model end to end, with the training step spread across workers."""

    retrieved = retrieve(
        start_date=start_date,
        end_date=end_date,
        feast_repo=feast_repo,
        data_table=data_table,
    )
    _with_lakehouse_credentials(retrieved)

    retrieved.set_memory_request("4Gi").set_memory_limit("8Gi")
    retrieved.set_cpu_request("2").set_cpu_limit("4")

    prepared = prepare(frame=retrieved.outputs["frame"])

    splits = split(
        prepared=prepared.outputs["prepared"],
        train_end=train_end,
        validation_end=validation_end,
    )

    # The baseline exists to prove the bigger model earns its complexity, so it
    # runs beside the real training rather than after it.
    baseline = train_baseline(
        train=splits.outputs["train"],
        feature_names=prepared.outputs["feature_names"],
    )

    distributed = train_distributed(
        train=splits.outputs["train"],
        validation=splits.outputs["validation"],
        feature_names=prepared.outputs["feature_names"],
        num_nodes=num_nodes,
        max_depth=max_depth,
        learning_rate=learning_rate,
        num_boost_round=num_boost_round,
        minio_endpoint=minio_endpoint,
        bucket=HANDOFF_BUCKET,
        prefix=HANDOFF_PREFIX,
        training_image=training_image,
    )
    _with_lakehouse_credentials(distributed)

    # The test split enters the graph here and nowhere else.
    evaluated = evaluate(
        validation=splits.outputs["validation"],
        test=splits.outputs["test"],
        feature_names=prepared.outputs["feature_names"],
        xgboost_model=distributed.outputs["model"],
        baseline_model=baseline.outputs["model"],
    )

    save_bundle(
        xgboost_model=distributed.outputs["model"],
        feature_names=prepared.outputs["feature_names"],
        evaluation=evaluated.outputs["threshold_out"],
    )

    # Records the model against the data version it came from, so a model
    # version can be traced back to the rows it was trained on.
    register_model(
        xgboost_model=distributed.outputs["model"],
        evaluation=evaluated.outputs["threshold_out"],
        data_snapshot_id=retrieved.outputs["data_snapshot_id"],
        data_table=data_table,
        mlflow_uri=mlflow_uri,
        num_nodes=num_nodes,
        max_depth=max_depth,
        learning_rate=learning_rate,
        num_boost_round=num_boost_round,
    )
