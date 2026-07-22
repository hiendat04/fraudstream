from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from fraudstream_datahub.model import (
    DATASETS_BY_NAME,
    PIPELINES,
    evaluate_contracts,
    load_contracts,
    validate_contract_schema,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_DIR = PROJECT_ROOT / "datahub" / "contracts"


class GovernanceModelTests(unittest.TestCase):
    def test_pipeline_contracts_cover_every_output_table(self) -> None:
        contracts = load_contracts(CONTRACT_DIR)
        validate_contract_schema(PROJECT_ROOT, contracts)

        self.assertEqual(
            [pipeline.pipeline_id for pipeline in PIPELINES],
            [
                "fraudstream_raw_to_bronze",
                "fraudstream_bronze_to_silver_gold",
                "fraudstream_offline_features",
            ],
        )
        self.assertEqual(len(contracts), 3)
        covered = {dataset.name for contract in contracts for dataset in contract.datasets}
        outputs = {dataset.name for pipeline in PIPELINES for dataset in pipeline.outputs}
        self.assertEqual(covered, outputs)
        self.assertEqual(len(DATASETS_BY_NAME), len(outputs) + 1)  # raw source is an input

    def test_measured_validation_results_pass_for_consistent_artifacts(self) -> None:
        contracts = load_contracts(CONTRACT_DIR)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_validation_artifacts(root)
            results = evaluate_contracts(root, contracts)

        self.assertEqual(len(results), 8)
        self.assertEqual({result.status for result in results}, {"SUCCESS"})

    def test_missing_artifacts_are_reported_as_error_not_success(self) -> None:
        contracts = load_contracts(CONTRACT_DIR)
        with tempfile.TemporaryDirectory() as temp_dir:
            results = evaluate_contracts(Path(temp_dir), contracts)

        self.assertEqual(len(results), 8)
        self.assertEqual({result.status for result in results}, {"ERROR"})


def _write_validation_artifacts(root: Path) -> None:
    _write_json(
        root / "data/bronze/raw_transactions/_bronze_validation_summary.json",
        {
            "passed": True,
            "source": {"row_count": 110, "csv_file_count": 3},
            "bronze": {"row_count": 110, "distinct_source_file_count": 3},
        },
    )
    _write_json(
        root / "data/silver/transactions/_silver_transactions_summary.json",
        {
            "input_row_count": 110,
            "output_row_count": 100,
            "quarantined_row_count": 0,
            "duplicate_rows_removed_count": 10,
            "warning_row_count": 5,
            "valid_row_count": 95,
        },
    )
    core_tables = [
        "dim_date",
        "dim_city",
        "dim_channel",
        "dim_quality_issue",
        "dim_merchant_category",
        "dim_customer",
        "dim_account",
        "dim_merchant",
        "fact_transactions",
        "fact_transaction_quality_issue",
        "fact_customer_daily",
        "fact_account_daily",
        "fact_merchant_daily",
        "fact_city_category_daily",
        "fact_device_ip_daily",
    ]
    _write_json(
        root / "data/gold/_gold_transactions_summary.json",
        {
            "build_scope": "core",
            "fact_transaction_count": 100,
            "tables": [{"table_name": name, "row_count": 1} for name in core_tables],
        },
    )
    _write_json(
        root / "data/gold/_offline_features_summary.json",
        {
            "source_fact_transaction_count": 100,
            "training_row_count": 100,
            "training_distinct_transaction_count": 100,
            "training_row_count_matches_fact": True,
            "training_transaction_ids_are_unique": True,
        },
    )


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
