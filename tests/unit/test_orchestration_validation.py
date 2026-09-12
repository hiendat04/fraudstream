"""Unit tests for scheduler-independent orchestration quality gates."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main

from fraudstream.jobs.gold.transactions import (
    CORE_GOLD_TABLE_NAMES,
    OFFLINE_FEATURE_TABLE_NAMES,
)
from fraudstream.orchestration.validation import (
    PipelineValidationError,
    validate_bronze_report,
    validate_core_gold_summary,
    validate_feature_store_materialization,
    validate_offline_feature_summary,
    validate_silver_summary,
    validate_source_manifest,
)


class OrchestrationValidationTest(TestCase):
    """Quality gates should accept reconciled artifacts and reject bad counts."""

    def test_validates_source_manifest_and_bronze_reconciliation(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source_dir = root / "data" / "raw"
            source_file = source_dir / "partition" / "transactions.csv"
            source_file.parent.mkdir(parents=True)
            source_file.write_text("transaction_id\ntxn_001\n", encoding="utf-8")
            _write_json(
                source_dir / "_manifest.json",
                {"files": [f"file://{source_file}"]},
            )
            _write_json(
                source_dir / "_quality_summary.json",
                {"written_file_count": 1, "row_count_after_duplicates": 1},
            )
            report_path = root / "bronze_validation.json"
            _write_json(
                report_path,
                {
                    "passed": True,
                    "source": {"row_count": 1, "csv_file_count": 1},
                    "bronze": {"row_count": 1, "distinct_source_file_count": 1},
                },
            )

            self.assertEqual(
                validate_source_manifest(source_dir),
                {"file_count": 1, "row_count": 1},
            )
            self.assertEqual(
                validate_bronze_report(report_path),
                {"source_rows": 1, "bronze_rows": 1},
            )

    def test_rejects_unaccounted_silver_rows(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            summary_path = root / "silver_summary.json"
            quality_path = root / "quality.json"
            _write_json(
                summary_path,
                {
                    "input_row_count": 10,
                    "output_row_count": 8,
                    "quarantined_row_count": 0,
                    "duplicate_rows_removed_count": 1,
                    "warning_row_count": 2,
                    "valid_row_count": 6,
                },
            )
            _write_json(quality_path, {"row_counts": {"input": 10, "silver_output": 8}})

            with self.assertRaisesRegex(PipelineValidationError, "row accounting"):
                validate_silver_summary(summary_path, quality_path)

    def test_validates_core_gold_and_offline_feature_grain(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            gold_dir = root / "gold"
            silver_summary_path = root / "silver_summary.json"
            _write_json(silver_summary_path, {"output_row_count": 5})
            core_tables = []
            for table_name in CORE_GOLD_TABLE_NAMES:
                core_tables.append(
                    {
                        "table_name": table_name,
                        "row_count": 5 if table_name == "fact_transactions" else 1,
                    }
                )
            _write_json(
                gold_dir / "_gold_transactions_summary.json",
                {
                    "build_scope": "core",
                    "fact_transaction_count": 5,
                    "tables": core_tables,
                },
            )

            self.assertEqual(
                validate_core_gold_summary(gold_dir, silver_summary_path),
                {"silver_rows": 5, "fact_transaction_rows": 5},
            )

            feature_tables = []
            for table_name in OFFLINE_FEATURE_TABLE_NAMES:
                feature_tables.append(
                    {
                        "table_name": table_name,
                        "row_count": 5,
                    }
                )
            _write_json(
                gold_dir / "_offline_features_summary.json",
                {
                    "source_fact_transaction_count": 5,
                    "training_row_count": 5,
                    "training_distinct_transaction_count": 5,
                    "training_row_count_matches_fact": True,
                    "training_transaction_ids_are_unique": True,
                    "tables": feature_tables,
                },
            )

            self.assertEqual(
                validate_offline_feature_summary(gold_dir),
                {"fact_transaction_rows": 5, "training_rows": 5},
            )

    def _materialization_summary(self, *, non_null=5, view_names=None, spot_check_views=None):
        """Build a materialization summary fixture with the given watermark and spot-check state."""

        names = view_names or (
            "customer_rolling_features",
            "customer_orders_90d_features",
            "merchant_risk_features",
            "customer_features_5m_stream",
            "merchant_features_5m_stream",
        )
        views = spot_check_views or [
            {"name": "customer_rolling_features", "entity_type": "customer", "requested": 5, "non_null": non_null},
            {"name": "customer_orders_90d_features", "entity_type": "customer", "requested": 5, "non_null": non_null},
            {"name": "merchant_risk_features", "entity_type": "merchant", "requested": 5, "non_null": non_null},
        ]
        return {
            "end_timestamp": "2026-09-09T00:00:00+00:00",
            "feature_views": [
                {"name": name, "mode": "full", "watermark_after": "2026-09-09T00:00:00+00:00"}
                for name in names
            ],
            "online_spot_check": {
                "views": views,
                "requested": sum(view["requested"] for view in views),
                "non_null": sum(view["non_null"] for view in views),
            },
        }

    def _write_summary(self, tmp_dir, summary):
        """Write one summary fixture to disk and return its path."""

        summary_path = Path(tmp_dir) / "summary.json"
        summary_path.write_text(json.dumps(summary), encoding="utf-8")
        return summary_path

    def test_feature_store_materialization_gate_accepts_a_complete_run(self):
        """Every expected view materialized and every spot-checked view found values."""

        with TemporaryDirectory() as tmp_dir:
            path = self._write_summary(tmp_dir, self._materialization_summary())

            result = validate_feature_store_materialization(path)

        self.assertEqual(result["feature_view_count"], 5)
        self.assertEqual(result["online_non_null_count"], 15)

    def test_feature_store_materialization_gate_rejects_a_missing_view(self):
        """A view that failed to materialize must stop the asset from being emitted."""

        with TemporaryDirectory() as tmp_dir:
            path = self._write_summary(
                tmp_dir, self._materialization_summary(view_names=("customer_rolling_features",))
            )

            with self.assertRaises(ValueError):
                validate_feature_store_materialization(path)

    def test_feature_store_materialization_gate_rejects_an_empty_online_store(self):
        """Materialization that writes nothing readable anywhere is a silent failure; catch it here."""

        with TemporaryDirectory() as tmp_dir:
            path = self._write_summary(tmp_dir, self._materialization_summary(non_null=0))

            with self.assertRaises(ValueError):
                validate_feature_store_materialization(path)

    def test_feature_store_materialization_gate_rejects_a_single_empty_view(self):
        """One empty view must fail the gate even while the aggregate and the rest look healthy."""

        spot_check_views = [
            {"name": "customer_rolling_features", "entity_type": "customer", "requested": 5, "non_null": 5},
            {"name": "customer_orders_90d_features", "entity_type": "customer", "requested": 5, "non_null": 0},
            {"name": "merchant_risk_features", "entity_type": "merchant", "requested": 5, "non_null": 5},
        ]
        with TemporaryDirectory() as tmp_dir:
            path = self._write_summary(
                tmp_dir, self._materialization_summary(spot_check_views=spot_check_views)
            )

            with self.assertRaisesRegex(ValueError, "customer_orders_90d_features"):
                validate_feature_store_materialization(path)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


if __name__ == "__main__":
    main()
