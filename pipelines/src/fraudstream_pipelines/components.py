"""The steps of the training pipeline, one per stage of the notebook.

Every step runs the same image and calls the already-tested fraudstream_ml
package rather than restating its logic. Imports sit inside the functions
because Kubeflow ships each function body to the cluster on its own.
"""

from typing import NamedTuple

from kfp import dsl
from kfp.dsl import Dataset, Input, Markdown, Metrics, Model, Output

TRAINING_IMAGE = "fraudstream-ml:dev"

# Where the training workers and the pipeline meet. The workers run as their own
# pods outside Kubeflow Pipelines, so they cannot read pipeline artifacts --
# object storage is the one place both sides can reach.
HANDOFF_BUCKET = "fraudstream"
HANDOFF_PREFIX = "pipeline-runs"


@dsl.component(base_image=TRAINING_IMAGE)
def retrieve(
    start_date: str,
    end_date: str,
    feast_repo: str,
    data_table: str,
    frame: Output[Dataset],
    coverage: Output[Metrics],
) -> NamedTuple("Outputs", [("data_snapshot_id", str)]):
    """Get features from the feature store, attach the labels, and save a data version.

    Returns the snapshot id as text. Snapshot ids are too large to survive being
    passed around as numbers, so they are kept as strings everywhere.
    """

    from pyspark.sql import SparkSession

    from fraudstream_ml.dataset import coverage_report, load_training_frame
    from fraudstream_ml.versioning import write_snapshot
    from typing import NamedTuple

    result = load_training_frame(feast_repo, start_date, end_date)
    result.to_parquet(frame.path)

    coverage.log_metric("rows", float(len(result)))
    coverage.log_metric("fraud_rate", float(result["is_fraud"].mean()))
    for view, share in coverage_report(result).items():
        coverage.log_metric(f"coverage_{view}", float(share))

    # Feast leaves its Spark session running, already pointed at the lakehouse.
    # Reusing it saves starting a second one just to write the table.
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("no Spark session after retrieval, so the data cannot be versioned")

    snapshot_id = write_snapshot(spark, result, table=data_table)
    print(f"saved {len(result)} rows as snapshot {snapshot_id}", flush=True)

    outputs = NamedTuple("Outputs", [("data_snapshot_id", str)])
    return outputs(str(snapshot_id))


@dsl.component(base_image=TRAINING_IMAGE)
def prepare(
    frame: Input[Dataset],
    prepared: Output[Dataset],
    feature_names: Output[Dataset],
) -> None:
    """Add availability flags, request-time features and encoded categories."""

    import json

    import pandas as pd

    from fraudstream_ml.features import prepare_features

    result, names = prepare_features(pd.read_parquet(frame.path))
    result.to_parquet(prepared.path)
    with open(feature_names.path, "w") as handle:
        json.dump(names, handle)


@dsl.component(base_image=TRAINING_IMAGE)
def split(
    prepared: Input[Dataset],
    train_end: str,
    validation_end: str,
    train: Output[Dataset],
    validation: Output[Dataset],
    test: Output[Dataset],
    summary: Output[Markdown],
) -> None:
    """Cut the frame into train, validation and test by event time."""

    import pandas as pd

    from fraudstream_ml.split import chronological_split, split_summary

    splits = chronological_split(pd.read_parquet(prepared.path), train_end, validation_end)
    splits["train"].to_parquet(train.path)
    splits["validation"].to_parquet(validation.path)
    splits["test"].to_parquet(test.path)

    with open(summary.path, "w") as handle:
        handle.write(split_summary(splits).to_markdown(index=False))


@dsl.component(base_image=TRAINING_IMAGE)
def train_baseline(
    train: Input[Dataset],
    feature_names: Input[Dataset],
    model: Output[Model],
) -> None:
    """Fit the logistic regression the gradient-boosted model has to beat."""

    import json

    import joblib
    import pandas as pd

    from fraudstream_ml.train import train_baseline as fit_baseline

    frame = pd.read_parquet(train.path)
    with open(feature_names.path) as handle:
        names = json.load(handle)

    joblib.dump(fit_baseline(frame[names], frame["is_fraud"]), model.path)


@dsl.component(base_image=TRAINING_IMAGE)
def train_distributed(
    train: Input[Dataset],
    validation: Input[Dataset],
    feature_names: Input[Dataset],
    num_nodes: int,
    max_depth: int,
    learning_rate: float,
    num_boost_round: int,
    minio_endpoint: str,
    bucket: str,
    prefix: str,
    training_image: str,
    model: Output[Model],
) -> None:
    """Run the training as a Kubeflow TrainJob spread over several workers.

    This step does not train anything itself. It stages the data somewhere the
    worker pods can read, asks Kubeflow to start them, waits, and collects the
    model they produce.
    """

    import json
    import os
    import uuid

    import pyarrow.fs as pafs
    from kubeflow.trainer import CustomTrainer, TrainerClient

    def storage() -> pafs.S3FileSystem:
        return pafs.S3FileSystem(
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            endpoint_override=minio_endpoint,
            scheme="http",
        )

    run_prefix = f"{bucket}/{prefix}/{uuid.uuid4().hex[:12]}"
    store = storage()
    for name, artifact in (("train", train), ("validation", validation)):
        with open(artifact.path, "rb") as source, store.open_output_stream(
            f"{run_prefix}/{name}.parquet"
        ) as sink:
            sink.write(source.read())
    with open(feature_names.path) as handle:
        names = json.load(handle)

    def training_function() -> None:
        """Runs inside each worker pod, once per worker."""

        import json
        import os

        import pandas as pd
        import pyarrow.fs as pafs

        from fraudstream_ml.distributed import train_distributed, worker_identity

        rank, world_size, tracker_uri, tracker_port = worker_identity()
        store = pafs.S3FileSystem(
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            endpoint_override=os.environ["MINIO_ENDPOINT_HOST"],
            scheme="http",
        )
        run_prefix = os.environ["RUN_PREFIX"]
        names = json.loads(os.environ["FEATURE_NAMES"])

        def read(name: str):
            with store.open_input_file(f"{run_prefix}/{name}.parquet") as source:
                return pd.read_parquet(source)

        train_frame, validation_frame = read("train"), read("validation")
        print(f"worker {rank} of {world_size} starting", flush=True)

        booster = train_distributed(
            train_frame[names],
            train_frame["is_fraud"],
            validation_frame[names],
            validation_frame["is_fraud"],
            rank=rank,
            world_size=world_size,
            tracker_uri=tracker_uri,
            tracker_port=tracker_port,
            params={
                "max_depth": int(os.environ["MAX_DEPTH"]),
                "learning_rate": float(os.environ["LEARNING_RATE"]),
            },
            num_boost_round=int(os.environ["NUM_BOOST_ROUND"]),
            model_path="/tmp/model.json",
        )

        if rank == 0:
            booster.save_model("/tmp/model.json")
            with open("/tmp/model.json", "rb") as source, store.open_output_stream(
                f"{run_prefix}/model.json"
            ) as sink:
                sink.write(source.read())
            print(f"worker 0 wrote a model of {booster.num_boosted_rounds()} trees", flush=True)

    client = TrainerClient()
    job = client.train(
        runtime=client.get_runtime("xgboost-distributed"),
        trainer=CustomTrainer(
            func=training_function,
            image=training_image,
            num_nodes=num_nodes,
            env={
                "MINIO_ACCESS_KEY": os.environ["MINIO_ACCESS_KEY"],
                "MINIO_SECRET_KEY": os.environ["MINIO_SECRET_KEY"],
                "MINIO_ENDPOINT_HOST": minio_endpoint,
                "RUN_PREFIX": run_prefix,
                "FEATURE_NAMES": json.dumps(names),
                "MAX_DEPTH": str(max_depth),
                "LEARNING_RATE": str(learning_rate),
                "NUM_BOOST_ROUND": str(num_boost_round),
            },
        ),
    )
    print(f"submitted TrainJob {job} across {num_nodes} workers", flush=True)

    client.wait_for_job_status(job)
    print("\n".join(client.get_job_logs(name=job)), flush=True)

    with store.open_input_file(f"{run_prefix}/model.json") as source, open(
        model.path, "wb"
    ) as sink:
        sink.write(source.read())


@dsl.component(base_image=TRAINING_IMAGE)
def evaluate(
    validation: Input[Dataset],
    test: Input[Dataset],
    feature_names: Input[Dataset],
    xgboost_model: Input[Model],
    baseline_model: Input[Model],
    metrics: Output[Metrics],
    report: Output[Markdown],
    threshold_out: Output[Dataset],
) -> None:
    """Choose the cut-off on validation, then score the test split exactly once."""

    import json

    import joblib
    import pandas as pd

    from fraudstream_ml.distributed import booster_to_classifier
    from fraudstream_ml.train import evaluate as score
    from fraudstream_ml.train import select_threshold

    with open(feature_names.path) as handle:
        names = json.load(handle)
    validation_frame = pd.read_parquet(validation.path)
    test_frame = pd.read_parquet(test.path)

    model = booster_to_classifier(xgboost_model.path)
    threshold = select_threshold(
        validation_frame["is_fraud"], model.predict_proba(validation_frame[names])[:, 1]
    )

    results = score(model, test_frame[names], test_frame["is_fraud"], threshold)
    baseline = score(
        joblib.load(baseline_model.path), test_frame[names], test_frame["is_fraud"], 0.5
    )

    for name, value in results.items():
        metrics.log_metric(name, float(value))
    metrics.log_metric("baseline_pr_auc", float(baseline["pr_auc"]))
    metrics.log_metric("baseline_roc_auc", float(baseline["roc_auc"]))

    with open(threshold_out.path, "w") as handle:
        json.dump({"threshold": threshold, "metrics": results}, handle)

    with open(report.path, "w") as handle:
        handle.write(
            pd.DataFrame(
                [
                    {"model": "xgboost (distributed)", **results},
                    {"model": "logistic regression", **baseline},
                ]
            ).to_markdown(index=False)
        )


@dsl.component(base_image=TRAINING_IMAGE)
def save_bundle(
    xgboost_model: Input[Model],
    feature_names: Input[Dataset],
    evaluation: Input[Dataset],
    bundle: Output[Model],
) -> None:
    """Save the model with the column order and cut-off needed to serve it."""

    import json

    from fraudstream_ml.distributed import booster_to_classifier
    from fraudstream_ml.train import save_bundle as write_bundle

    with open(feature_names.path) as handle:
        names = json.load(handle)
    with open(evaluation.path) as handle:
        evaluated = json.load(handle)

    write_bundle(
        bundle.path,
        model=booster_to_classifier(xgboost_model.path),
        feature_names=names,
        threshold=evaluated["threshold"],
        metrics=evaluated["metrics"],
    )


@dsl.component(base_image=TRAINING_IMAGE)
def register_model(
    xgboost_model: Input[Model],
    evaluation: Input[Dataset],
    data_snapshot_id: str,
    data_table: str,
    mlflow_uri: str,
    num_nodes: int,
    max_depth: int,
    learning_rate: float,
    num_boost_round: int,
) -> None:
    """Save the trained model to MLflow with its settings, scores and data version.

    This adds a new model version. It does not make it the production model.
    """

    import json

    from fraudstream_ml.distributed import booster_to_classifier
    from fraudstream_ml.registry import log_training_run

    with open(evaluation.path) as handle:
        evaluated = json.load(handle)

    recorded = log_training_run(
        booster_to_classifier(xgboost_model.path),
        params={
            "max_depth": max_depth,
            "learning_rate": learning_rate,
            "num_boost_round": num_boost_round,
            "num_nodes": num_nodes,
            "threshold": evaluated["threshold"],
        },
        metrics=evaluated["metrics"],
        data_snapshot_id=data_snapshot_id,
        data_table=data_table,
        tracking_uri=mlflow_uri,
    )
    print(f"registered version {recorded.model_version} from run {recorded.run_id}", flush=True)
