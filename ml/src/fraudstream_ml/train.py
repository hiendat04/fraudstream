"""Train, tune, evaluate, and persist the fraud classifier."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier


BASE_PARAMS: dict[str, Any] = {
    "objective": "binary:logistic",
    "eval_metric": "aucpr",
    "tree_method": "hist",
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 5,
}

TUNING_GRID: tuple[dict[str, Any], ...] = (
    {"max_depth": 3, "learning_rate": 0.05, "min_child_weight": 5},
    {"max_depth": 3, "learning_rate": 0.10, "min_child_weight": 5},
    {"max_depth": 3, "learning_rate": 0.20, "min_child_weight": 5},
    {"max_depth": 5, "learning_rate": 0.05, "min_child_weight": 5},
    {"max_depth": 5, "learning_rate": 0.10, "min_child_weight": 5},
    {"max_depth": 5, "learning_rate": 0.20, "min_child_weight": 5},
    {"max_depth": 7, "learning_rate": 0.05, "min_child_weight": 10},
    {"max_depth": 7, "learning_rate": 0.10, "min_child_weight": 10},
    {"max_depth": 7, "learning_rate": 0.20, "min_child_weight": 10},
    {"max_depth": 5, "learning_rate": 0.10, "min_child_weight": 20},
)


def scale_pos_weight(labels: Any) -> float:
    """Return negatives divided by positives, XGBoost's class-imbalance lever."""

    positives = float(np.sum(labels))
    if positives == 0:
        raise ValueError("cannot weight a label column with no positive class")
    return float(len(labels) - positives) / positives


def train_xgboost(
    train_features: Any,
    train_labels: Any,
    validation_features: Any,
    validation_labels: Any,
    *,
    params: dict[str, Any] | None = None,
    n_estimators: int = 400,
    early_stopping_rounds: int = 50,
    random_state: int = 42,
) -> XGBClassifier:
    """Fit a gradient-boosted classifier, early-stopping on the validation split."""

    settings = {**BASE_PARAMS, **(params or {})}
    model = XGBClassifier(
        **settings,
        n_estimators=n_estimators,
        early_stopping_rounds=early_stopping_rounds,
        scale_pos_weight=scale_pos_weight(train_labels),
        random_state=random_state,
    )
    model.fit(
        train_features,
        train_labels,
        eval_set=[(validation_features, validation_labels)],
        verbose=False,
    )
    return model


def train_baseline(
    train_features: Any,
    train_labels: Any,
    *,
    random_state: int = 42,
) -> object:
    """Fit the logistic-regression baseline the tree model has to beat."""

    return Pipeline(
        [
            ("impute", SimpleImputer(strategy="median")),
            ("scale", StandardScaler()),
            (
                "model",
                LogisticRegression(
                    class_weight="balanced", max_iter=1000, random_state=random_state
                ),
            ),
        ]
    ).fit(train_features, train_labels)


def tune_xgboost(
    train_features: Any,
    train_labels: Any,
    validation_features: Any,
    validation_labels: Any,
    *,
    grid: tuple[dict[str, Any], ...] = TUNING_GRID,
    n_estimators: int = 400,
    early_stopping_rounds: int = 50,
    random_state: int = 42,
) -> tuple[XGBClassifier, Any]:
    """Fit every grid config and return the best model by validation PR-AUC, plus the full table."""

    models: list[XGBClassifier] = []
    rows: list[dict[str, Any]] = []
    for config in grid:
        model = train_xgboost(
            train_features,
            train_labels,
            validation_features,
            validation_labels,
            params=config,
            n_estimators=n_estimators,
            early_stopping_rounds=early_stopping_rounds,
            random_state=random_state,
        )
        metrics = evaluate(model, validation_features, validation_labels)
        models.append(model)
        rows.append(
            {
                **config,
                "validation_pr_auc": metrics["pr_auc"],
                "validation_roc_auc": metrics["roc_auc"],
                "best_iteration": int(getattr(model, "best_iteration", 0) or 0),
            }
        )

    best_model = models[int(np.argmax([row["validation_pr_auc"] for row in rows]))]
    results = (
        pd.DataFrame(rows)
        .sort_values("validation_pr_auc", ascending=False)
        .reset_index(drop=True)
    )
    return best_model, results


def select_threshold(labels: Any, scores: Any) -> float:
    """Return the probability cut-off that maximizes F1 on the given scores."""

    precision, recall, thresholds = precision_recall_curve(labels, scores)
    if len(thresholds) ==   0:
        return 0.5

    precision, recall = precision[:-1], recall[:-1]
    total = precision + recall
    f1 = np.divide(2 * precision * recall, total, out=np.zeros_like(total), where=total > 0)
    return float(thresholds[int(np.argmax(f1))])


def evaluate(model: Any, features: Any, labels: Any, threshold: float = 0.5) -> dict[str, float]:
    """Score a fitted model, leading with the metrics that survive class imbalance."""

    scores = model.predict_proba(features)[:, 1]
    predictions = (scores >= threshold).astype(int)
    return {
        "pr_auc": float(average_precision_score(labels, scores)),
        "roc_auc": float(roc_auc_score(labels, scores)),
        "precision": float(precision_score(labels, predictions, zero_division=0)),
        "recall": float(recall_score(labels, predictions, zero_division=0)),
        "f1": float(f1_score(labels, predictions, zero_division=0)),
        "accuracy": float(accuracy_score(labels, predictions)),
        "threshold": float(threshold),
        "positives": int(np.sum(labels)),
        "predicted_positives": int(predictions.sum()),
    }


def save_bundle(
    path: str | Path,
    *,
    model: Any,
    feature_names: list[str],
    threshold: float,
    metrics: dict[str, float],
) -> Path:
    """Persist the model together with the column order and threshold needed to serve it."""

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {
            "model": model,
            "feature_names": list(feature_names),
            "threshold": float(threshold),
            "metrics": dict(metrics),
            "trained_at": datetime.now(timezone.utc).isoformat(),
        },
        destination,
    )
    return destination


def load_bundle(path: str | Path) -> dict[str, Any]:
    """Load a saved model bundle."""

    return joblib.load(Path(path))
