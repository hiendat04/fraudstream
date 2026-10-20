from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from fraudstream_datahub.model import (
    DATASETS_BY_NAME,
    PIPELINES,
    ContractError,
    evaluate_contracts,
    load_contracts,
    pipeline_datasets,
    selected_pipelines,
    validate_contract_schema,
)
from fraudstream_datahub.publish import build_report


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
                "fraudstream_rag_index",
            ],
        )
        self.assertEqual(len(contracts), 4)
        covered = {dataset.name for contract in contracts for dataset in contract.datasets}
        outputs = {dataset.name for pipeline in PIPELINES for dataset in pipeline.outputs}
        self.assertEqual(covered, outputs)
        # The raw source and the three RAG sources are only ever inputs.
        self.assertEqual(len(DATASETS_BY_NAME), len(outputs) + 4)

    def test_measured_validation_results_pass_for_consistent_artifacts(self) -> None:
        contracts = load_contracts(CONTRACT_DIR)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_validation_artifacts(root)
            results = evaluate_contracts(root, contracts)

        self.assertEqual(len(results), 12)
        self.assertEqual({result.status for result in results}, {"SUCCESS"})

    def test_missing_artifacts_are_reported_as_error_not_success(self) -> None:
        contracts = load_contracts(CONTRACT_DIR)
        with tempfile.TemporaryDirectory() as temp_dir:
            results = evaluate_contracts(Path(temp_dir), contracts)

        self.assertEqual(len(results), 12)
        self.assertEqual({result.status for result in results}, {"ERROR"})

    def test_every_pipeline_together_reads_and_writes_every_dataset(self) -> None:
        self.assertEqual(
            {dataset.name for dataset in pipeline_datasets(PIPELINES)}, set(DATASETS_BY_NAME)
        )

    def test_the_rag_pipeline_can_be_published_on_its_own(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _write_json(root / "data/rag/_rag_index_summary.json", RAG_SUMMARY)
            _, results, report = build_report(root, CONTRACT_DIR, "PROD", pipeline_id="fraudstream_rag_index")

        self.assertEqual(report["pipelines"], ["fraudstream_rag_index"])
        self.assertEqual((report["contract_count"], report["dataset_count"]), (1, 5))
        self.assertEqual(report["assertion_status_counts"], {"SUCCESS": 4, "FAILURE": 0, "ERROR": 0})
        self.assertEqual(len(results), 4)

    def test_an_unknown_pipeline_is_refused(self) -> None:
        with self.assertRaises(ContractError):
            selected_pipelines("fraudstream_nothing")

    def test_the_rag_contract_needs_no_postgres_ddl(self) -> None:
        contracts = tuple(
            contract for contract in load_contracts(CONTRACT_DIR)
            if contract.pipeline_id == "fraudstream_rag_index"
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            validate_contract_schema(Path(temp_dir), contracts)

    def test_rag_checks_fail_when_a_run_went_wrong(self) -> None:
        broken = {
            "rag-sources-present": {"validation": {"documents": {**RAG_SUMMARY["validation"]["documents"], "handbook": 0}, "problems": ["no documents from handbook"]}},
            "rag-chunks-embedded": {"embed": {**RAG_SUMMARY["embed"], "dimensions": [384]}},
            "rag-stores-agree": {"online": {"rows": 70}},
            "rag-golden-retrieval": {"retrieval": {**RAG_SUMMARY["retrieval"], "hits": 9, "passed": False}},
        }
        contracts = tuple(
            contract for contract in load_contracts(CONTRACT_DIR)
            if contract.pipeline_id == "fraudstream_rag_index"
        )
        for rule_id, change in broken.items():
            with self.subTest(rule_id), tempfile.TemporaryDirectory() as temp_dir:
                root = Path(temp_dir)
                _write_json(root / "data/rag/_rag_index_summary.json", {**RAG_SUMMARY, **change})
                statuses = {result.rule.rule_id: result.status for result in evaluate_contracts(root, contracts)}
                self.assertEqual(statuses[rule_id], "FAILURE")
                self.assertEqual(sum(status == "SUCCESS" for status in statuses.values()), 3)


RAG_SUMMARY = {
    "validation": {"documents": {"handbook": 7, "model_card": 5, "feature_dictionary": 5}, "problems": []},
    "embed": {"chunks": 71, "dimensions": [768], "min_norm": 1.0, "max_norm": 1.0},
    "offline": {"rows": 71},
    "online": {"rows": 71},
    "retrieval": {"questions": 12, "hits": 12, "top_k": 5, "min_hit_rate": 0.9, "passed": True},
}


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
    _write_json(root / "data/rag/_rag_index_summary.json", RAG_SUMMARY)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
