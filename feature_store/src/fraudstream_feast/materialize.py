"""Materialize offline feature rows into the online store, incrementally.

`FeatureStore.materialize_incremental` resumes from each feature view's stored
watermark, and when a view has never been materialized it falls back to a
window of `now - ttl`. For a historical dataset that fallback window can sit
entirely after the data, in which case the run succeeds while loading nothing.
This job therefore does one explicit full window per view the first time it
sees it, and resumes incrementally on every run afterwards.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from feast import FeatureStore

from enum import Enum


DEFAULT_FEATURE_VIEWS = (
    "customer_rolling_features",
    "customer_orders_90d_features",
    "merchant_risk_features",
    "customer_features_5m_stream",
    "merchant_features_5m_stream",
)
SPOT_CHECK_ENTITY_COUNT = 5

def plan_materialization(
    feature_views: list[Any],
    *,
    start_timestamp: datetime,
    end_timestamp: datetime,
) -> dict[str, list[str]]:
    """Split feature views into those needing a full first window and those resuming."""

    if end_timestamp <= start_timestamp:
        raise ValueError(
            f"end_timestamp ({end_timestamp.isoformat()}) must be after "
            f"start_timestamp ({start_timestamp.isoformat()})"
        )
    full = [view.name for view in feature_views if view.most_recent_end_time is None]
    incremental = [view.name for view in feature_views if view.most_recent_end_time is not None]
    return {"full": full, "incremental": incremental}


def materialize_features(
    *,
    repo_path: str,
    start_timestamp: datetime,
    end_timestamp: datetime,
    feature_view_names: tuple[str, ...] = DEFAULT_FEATURE_VIEWS,
    summary_path: Path | None = None,
) -> dict[str, Any]:
    """Materialize each feature view into the online store and return run evidence."""

    store = FeatureStore(repo_path=repo_path)
    views = [store.get_feature_view(name) for name in feature_view_names]
    watermarks_before = {view.name: view.most_recent_end_time for view in views}
    plan = plan_materialization(views, start_timestamp=start_timestamp, end_timestamp=end_timestamp)

    if plan["full"]:
        store.materialize(
            start_date=start_timestamp, end_date=end_timestamp, feature_views=plan["full"]
        )
    if plan["incremental"]:
        store.materialize_incremental(end_date=end_timestamp, feature_views=plan["incremental"])

    refreshed = [store.get_feature_view(name) for name in feature_view_names]
    summary: dict[str, Any] = {
        "repo_path": repo_path,
        "start_timestamp": start_timestamp.isoformat(),
        "end_timestamp": end_timestamp.isoformat(),
        "feature_views": [
            {
                "name": view.name,
                "mode": "full" if view.name in plan["full"] else "incremental",
                "ttl_seconds": int(view.ttl.total_seconds()) if view.ttl else None,
                "watermark_before": (
                    watermarks_before[view.name].isoformat() if watermarks_before[view.name] else None
                ),
                "watermark_after": (
                    view.most_recent_end_time.isoformat() if view.most_recent_end_time else None
                ),
            }
            for view in refreshed
        ],
        "online_spot_check": _spot_check_online_store(store),
    }
    if summary_path is not None:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def _spot_check_online_store(store: FeatureStore) -> dict[str, Any]:
    """Read a handful of real entities back out of the online store for every batch view.

    Watermarks alone are not proof: a run can advance every watermark and still
    leave Redis empty for some views while others read back fine. Checking only
    one view would let a regression in another hide behind a healthy aggregate,
    so every batch feature view this job materializes gets its own read-back.
    """

    from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
        get_spark_session_or_start_new_with_repoconfig,
    )

    spark = get_spark_session_or_start_new_with_repoconfig(store.config.offline_store)
    customer_ids = _sample_entity_ids(
        spark,
        "SELECT DISTINCT customer_id FROM iceberg.gold.feat_customer_rolling "
        f"WHERE customer_id IS NOT NULL LIMIT {SPOT_CHECK_ENTITY_COUNT}",
        "customer_id",
    )
    merchant_ids = _sample_entity_ids(
        spark,
        "SELECT DISTINCT merchant_dim_id AS merchant_id FROM iceberg.gold.feat_merchant_risk_rolling "
        f"WHERE merchant_dim_id IS NOT NULL LIMIT {SPOT_CHECK_ENTITY_COUNT}",
        "merchant_id",
    )

    views = [
        _spot_check_view(store, "customer_rolling_features", "txn_count_7d", "customer_id", "customer", customer_ids),
        _spot_check_view(
            store, "customer_orders_90d_features", "total_orders_90d", "customer_id", "customer", customer_ids
        ),
        _spot_check_view(
            store, "merchant_risk_features", "merchant_txn_count_1d", "merchant_id", "merchant", merchant_ids
        ),
    ]
    return {
        "views": views,
        "requested": sum(view["requested"] for view in views),
        "non_null": sum(view["non_null"] for view in views),
    }


def _sample_entity_ids(spark: Any, query: str, column: str) -> list[str]:
    """Return a handful of distinct entity ids from Gold to use as spot-check probes."""

    return [row[column] for row in spark.sql(query).collect()]


def _spot_check_view(
    store: FeatureStore,
    view_name: str,
    feature_name: str,
    join_key: str,
    entity_type: str,
    entity_ids: list[str],
) -> dict[str, Any]:
    """Read one feature back for a handful of real entities and count non-null results."""

    if not entity_ids:
        return {"name": view_name, "entity_type": entity_type, "requested": 0, "non_null": 0}

    response = store.get_online_features(
        features=[f"{view_name}:{feature_name}"],
        entity_rows=[{join_key: entity_id} for entity_id in entity_ids],
    ).to_dict()
    non_null = sum(1 for value in response[feature_name] if value is not None)
    return {"name": view_name, "entity_type": entity_type, "requested": len(entity_ids), "non_null": non_null}


def _parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp into a UTC-aware datetime."""

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def main(argv: list[str] | None = None) -> int:
    """Run incremental materialization from the command line."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-path", default=None, help="Feast repo directory. Defaults to $FEAST_REPO_PATH.")
    parser.add_argument(
        "--start-timestamp",
        required=True,
        help="Window start used only for feature views that have never been materialized.",
    )
    parser.add_argument("--end-timestamp", required=True, help="Window end for this run.")
    parser.add_argument("--summary-path", default=None, help="Where to write the run summary JSON.")
    args = parser.parse_args(argv)

    summary = materialize_features(
        repo_path=args.repo_path or os.environ.get("FEAST_REPO_PATH", "feature_store/feature_repo"),
        start_timestamp=_parse_timestamp(args.start_timestamp),
        end_timestamp=_parse_timestamp(args.end_timestamp),
        summary_path=Path(args.summary_path) if args.summary_path else None,
    )
    json.dump(summary, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
