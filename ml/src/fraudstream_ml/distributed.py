"""Train the fraud classifier across several workers using XGBoost's native API."""

from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import Any

import xgboost as xgb
from xgboost import collective as coll
from xgboost import XGBClassifier
from xgboost.tracker import RabitTracker

from fraudstream_ml.train import BASE_PARAMS, scale_pos_weight

# The native API spells a few settings differently, and rejects nothing it does
# not recognise -- an untranslated name is silently ignored, so a model would
# quietly train with the default learning rate instead of the tuned one.
SKLEARN_TO_NATIVE = {"learning_rate": "eta", "random_state": "seed"}

# These are arguments to xgboost.train itself, not model parameters.
NOT_PARAMETERS = frozenset({"n_estimators", "early_stopping_rounds"})


def shard_for_rank(data: Any, rank: int, world_size: int) -> Any:
    """Return the slice of the data this worker trains on.

    Rows are dealt out one at a time, so every worker gets an interleaved
    sample of the whole period rather than one contiguous block of it. That
    matters here because the data is ordered by time: contiguous blocks would
    hand each worker a different few weeks, and a worker whose weeks happened
    to contain little fraud would contribute almost nothing.
    """

    if world_size < 1:
        raise ValueError(f"world_size must be at least 1, got {world_size}")
    if not 0 <= rank < world_size:
        raise ValueError(f"rank {rank} is outside a world of {world_size}")
    return data.iloc[rank::world_size]


def native_params(params: dict[str, Any] | None, labels: Any) -> dict[str, Any]:
    """Translate the sklearn-style settings into what xgboost.train expects."""

    settings = {**BASE_PARAMS, **(params or {})}
    translated = {
        SKLEARN_TO_NATIVE.get(name, name): value
        for name, value in settings.items()
        if name not in NOT_PARAMETERS
    }
    translated["scale_pos_weight"] = scale_pos_weight(labels)
    return translated


def worker_identity() -> tuple[int, int, str, int]:
    """Read this worker's rank and the tracker's address from the environment.

    Kubeflow sets these on every training pod it starts.
    """

    return (
        int(os.environ["DMLC_TASK_ID"]),
        int(os.environ["DMLC_NUM_WORKER"]),
        os.environ["DMLC_TRACKER_URI"],
        int(os.environ["DMLC_TRACKER_PORT"]),
    )


def _free_port() -> int:
    """Pick an unused port, for running a single worker outside a cluster."""

    with socket.socket() as probe:
        probe.bind(("", 0))
        return int(probe.getsockname()[1])


def train_distributed(
    train_features: Any,
    train_labels: Any,
    validation_features: Any,
    validation_labels: Any,
    *,
    rank: int,
    world_size: int,
    tracker_uri: str = "127.0.0.1",
    tracker_port: int | None = None,
    params: dict[str, Any] | None = None,
    num_boost_round: int = 400,
    early_stopping_rounds: int = 50,
    model_path: str | Path | None = None,
) -> xgb.Booster:
    """Train one worker's share of the data, synchronizing with the others as it goes.

    Every worker runs this. Each holds a different slice of the rows, and they
    exchange split statistics at each step, so the trees they build are the
    trees a single machine would have built on all the data.
    """

    if len(train_features) != len(train_labels):
        raise ValueError(
            f"{len(train_features)} training rows but {len(train_labels)} labels"
        )
    if len(validation_features) != len(validation_labels):
        raise ValueError(
            f"{len(validation_features)} validation rows but {len(validation_labels)} labels"
        )

    if tracker_port is None:
        tracker_port = _free_port()

    # The first worker also runs the meeting point the others connect to.
    tracker = None
    if rank == 0:
        tracker = RabitTracker(host_ip="0.0.0.0", n_workers=world_size, port=tracker_port)
        tracker.start()

    # 
    with coll.CommunicatorContext(
        dmlc_tracker_uri=tracker_uri,
        dmlc_tracker_port=tracker_port,
        dmlc_task_id=str(rank),
    ):
        train_slice = shard_for_rank(train_features, rank, world_size)
        train_label_slice = shard_for_rank(train_labels, rank, world_size)
        validation_slice = shard_for_rank(validation_features, rank, world_size)
        validation_label_slice = shard_for_rank(validation_labels, rank, world_size)

        dtrain = xgb.QuantileDMatrix(train_slice, label=train_label_slice)
        dvalidation = xgb.QuantileDMatrix(
            validation_slice, label=validation_label_slice, ref=dtrain
        )

        booster = xgb.train(
            native_params(params, train_label_slice),
            dtrain,
            num_boost_round=num_boost_round,
            evals=[(dvalidation, "validation")],
            early_stopping_rounds=early_stopping_rounds,
            verbose_eval=False,
        )

        if model_path is not None and coll.get_rank() == 0:
            destination = Path(model_path)
            destination.parent.mkdir(parents=True, exist_ok=True)
            booster.save_model(destination)

    if tracker is not None:
        tracker.wait_for()
    return booster


def booster_to_classifier(model_path: str | Path) -> XGBClassifier:
    """Load a saved model back into the sklearn wrapper so evaluate() can score it.

    Evaluation asks for predict_proba, which the native Booster does not offer.
    """

    classifier = XGBClassifier()
    classifier.load_model(Path(model_path))
    return classifier
