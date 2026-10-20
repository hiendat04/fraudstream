"""Governance model shared by the DataHub publisher and its tests."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

import yaml


POSTGRES_PLATFORM = "postgres"
FILE_PLATFORM = "file"
DATABASE_NAME = "fraudstream"
DEFAULT_PLATFORM_INSTANCE = "fraudstream-local"


@dataclass(frozen=True)
class DatasetRef:
    """A DataHub dataset plus the minimum metadata needed to describe it."""

    platform: str
    name: str
    layer: str
    description: str

    def datahub_name(self, platform_instance: str = DEFAULT_PLATFORM_INSTANCE) -> str:
        if self.platform == POSTGRES_PLATFORM and platform_instance:
            return f"{platform_instance}.{self.name}"
        return self.name

    def urn(self, env: str, platform_instance: str = DEFAULT_PLATFORM_INSTANCE) -> str:
        return (
            f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},"
            f"{self.datahub_name(platform_instance)},{env})"
        )


@dataclass(frozen=True)
class PipelineSpec:
    """A governed Airflow pipeline and its table-level dependencies."""

    pipeline_id: str
    title: str
    dag_id: str
    description: str
    inputs: tuple[DatasetRef, ...]
    outputs: tuple[DatasetRef, ...]
    lineage: tuple[tuple[DatasetRef, DatasetRef], ...]


@dataclass(frozen=True)
class ContractDataset:
    """One dataset covered by a repository-owned data contract."""

    name: str
    grain: str
    required_fields: tuple[str, ...]


@dataclass(frozen=True)
class ContractRule:
    """A contract rule evaluated from a pipeline quality artifact."""

    rule_id: str
    target: str
    description: str
    evaluator: str
    severity: str


@dataclass(frozen=True)
class ContractSpec:
    """A validated FraudStream data-contract document."""

    contract_id: str
    version: str
    status: str
    owner: str
    pipeline_id: str
    source_path: Path
    datasets: tuple[ContractDataset, ...]
    rules: tuple[ContractRule, ...]


@dataclass(frozen=True)
class ValidationResult:
    """The result sent to a DataHub custom assertion."""

    rule: ContractRule
    status: str
    message: str
    properties: Mapping[str, str]


def postgres_dataset(schema: str, table: str, description: str) -> DatasetRef:
    return DatasetRef(
        platform=POSTGRES_PLATFORM,
        name=f"{DATABASE_NAME}.{schema}.{table}",
        layer=schema,
        description=description,
    )


RAW_SOURCE = DatasetRef(
    platform=FILE_PLATFORM,
    name="fraudstream.raw_source.offline_transactions",
    layer="source",
    description="Generated partitioned CSV transactions before Bronze ingestion.",
)

BRONZE_INGEST_RUNS = postgres_dataset(
    "bronze",
    "raw_transaction_ingest_runs",
    "One audit row per Bronze ingestion run.",
)
BRONZE_TRANSACTIONS = postgres_dataset(
    "bronze",
    "raw_transactions",
    "Source-faithful transaction rows with Bronze ingestion metadata.",
)
SILVER_TRANSACTIONS = postgres_dataset(
    "silver",
    "stg_transactions",
    "Typed, cleaned, and deterministically deduplicated transactions.",
)
SILVER_QUALITY_ISSUES = postgres_dataset(
    "silver",
    "stg_transaction_quality_issues",
    "Selected, quarantined, and duplicate-rejected quality evidence.",
)

CORE_GOLD_TABLE_NAMES = (
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
)
OFFLINE_FEATURE_TABLE_NAMES = (
    "feat_customer_rolling",
    "feat_customer_total_orders_90d",
    "feat_merchant_risk_rolling",
    "feat_transaction_training",
)

CORE_GOLD_DATASETS = tuple(
    postgres_dataset(
        "gold",
        table_name,
        f"Core Gold {table_name.replace('_', ' ')} table produced from validated Silver transactions.",
    )
    for table_name in CORE_GOLD_TABLE_NAMES
)
OFFLINE_FEATURE_DATASETS = tuple(
    postgres_dataset(
        "gold",
        table_name,
        f"Point-in-time offline feature table: {table_name.replace('_', ' ')}.",
    )
    for table_name in OFFLINE_FEATURE_TABLE_NAMES
)

RAG_SUMMARY = "data/rag/_rag_index_summary.json"
RAG_SOURCES = ("handbook", "model_card", "feature_dictionary")
RAG_DIMENSIONS = 768
RAG_HANDBOOK = DatasetRef(
    platform="s3",
    name="fraudstream/rag/handbook",
    layer="source",
    description="The fraud-operations handbook in Markdown, copied from Git by CI.",
)
RAG_MODEL_CARDS = DatasetRef(
    platform="mlflow",
    name="fraud-detection",
    layer="source",
    description="Every registered version of the fraud detection model, read as a model card.",
)
RAG_FEATURE_DICTIONARY = DatasetRef(
    platform="feast",
    name="fraudstream",
    layer="source",
    description="The fraudstream feature views in the Feast registry, read as a feature dictionary.",
)
RAG_CHUNKS = DatasetRef(
    platform="iceberg",
    name="rag.chunks",
    layer="rag",
    description="Every chunk the copilot can cite, with its embedding; one Iceberg snapshot per change.",
)
RAG_ONLINE_CHUNKS = DatasetRef(
    platform=POSTGRES_PLATFORM,
    name="llm.rag.fraudstream_rag_document_chunks",
    layer="rag",
    description="Feast's pgvector online store for the chunks, which the context API searches.",
)

DATASETS_BY_NAME = {
    dataset.name: dataset
    for dataset in (
        RAW_SOURCE,
        BRONZE_INGEST_RUNS,
        BRONZE_TRANSACTIONS,
        SILVER_TRANSACTIONS,
        SILVER_QUALITY_ISSUES,
        *CORE_GOLD_DATASETS,
        *OFFLINE_FEATURE_DATASETS,
        RAG_HANDBOOK,
        RAG_MODEL_CARDS,
        RAG_FEATURE_DICTIONARY,
        RAG_CHUNKS,
        RAG_ONLINE_CHUNKS,
    )
}


def _gold(name: str) -> DatasetRef:
    return DATASETS_BY_NAME[f"{DATABASE_NAME}.gold.{name}"]


PIPELINES = (
    PipelineSpec(
        pipeline_id="fraudstream_raw_to_bronze",
        title="Raw to Bronze",
        dag_id="fraudstream_raw_to_bronze",
        description="Generate offline source extracts, ingest source-faithful Bronze rows, and reconcile row and file counts.",
        inputs=(RAW_SOURCE,),
        outputs=(BRONZE_INGEST_RUNS, BRONZE_TRANSACTIONS),
        lineage=(
            (RAW_SOURCE, BRONZE_INGEST_RUNS),
            (RAW_SOURCE, BRONZE_TRANSACTIONS),
        ),
    ),
    PipelineSpec(
        pipeline_id="fraudstream_bronze_to_silver_gold",
        title="Bronze to Silver and Gold",
        dag_id="fraudstream_bronze_to_silver_gold",
        description="Clean and deduplicate Bronze transactions, preserve quality evidence, and build core Gold dimensions and facts.",
        inputs=(BRONZE_TRANSACTIONS,),
        outputs=(SILVER_TRANSACTIONS, SILVER_QUALITY_ISSUES, *CORE_GOLD_DATASETS),
        lineage=(
            (BRONZE_TRANSACTIONS, SILVER_TRANSACTIONS),
            (BRONZE_TRANSACTIONS, SILVER_QUALITY_ISSUES),
            *((SILVER_TRANSACTIONS, dataset) for dataset in CORE_GOLD_DATASETS),
        ),
    ),
    PipelineSpec(
        pipeline_id="fraudstream_offline_features",
        title="Offline Features",
        dag_id="fraudstream_offline_features",
        description="Read persisted Gold facts and materialize point-in-time-safe offline fraud features.",
        inputs=(
            _gold("fact_transactions"),
            _gold("fact_customer_daily"),
            _gold("fact_account_daily"),
            _gold("fact_merchant_daily"),
        ),
        outputs=OFFLINE_FEATURE_DATASETS,
        lineage=(
            (_gold("fact_customer_daily"), _gold("feat_customer_rolling")),
            (_gold("fact_customer_daily"), _gold("feat_customer_total_orders_90d")),
            (_gold("fact_transactions"), _gold("feat_merchant_risk_rolling")),
            (_gold("fact_merchant_daily"), _gold("feat_merchant_risk_rolling")),
            (_gold("fact_transactions"), _gold("feat_transaction_training")),
            (_gold("fact_account_daily"), _gold("feat_transaction_training")),
            (_gold("feat_customer_rolling"), _gold("feat_transaction_training")),
            (_gold("feat_merchant_risk_rolling"), _gold("feat_transaction_training")),
        ),
    ),
    PipelineSpec(
        pipeline_id="fraudstream_rag_index",
        title="RAG Index",
        dag_id="fraudstream_rag_index",
        description="Load the handbook, model cards and feature dictionary; validate, chunk and embed them; write Iceberg and pgvector; check retrieval.",
        inputs=(RAG_HANDBOOK, RAG_MODEL_CARDS, RAG_FEATURE_DICTIONARY),
        outputs=(RAG_CHUNKS, RAG_ONLINE_CHUNKS),
        lineage=(
            (RAG_HANDBOOK, RAG_CHUNKS),
            (RAG_MODEL_CARDS, RAG_CHUNKS),
            (RAG_FEATURE_DICTIONARY, RAG_CHUNKS),
            (RAG_CHUNKS, RAG_ONLINE_CHUNKS),
        ),
    ),
)


def selected_pipelines(pipeline_id: str | None) -> tuple[PipelineSpec, ...]:
    """Every pipeline, or just the one a DAG publishes for itself."""

    if pipeline_id is None:
        return PIPELINES
    chosen = tuple(pipeline for pipeline in PIPELINES if pipeline.pipeline_id == pipeline_id)
    if not chosen:
        raise ContractError(f"unknown pipeline: {pipeline_id}")
    return chosen


def pipeline_datasets(pipelines: tuple[PipelineSpec, ...]) -> tuple[DatasetRef, ...]:
    """The datasets these pipelines read or write, each once."""

    found: dict[str, DatasetRef] = {}
    for pipeline in pipelines:
        for dataset in (*pipeline.inputs, *pipeline.outputs):
            found.setdefault(dataset.name, dataset)
    return tuple(found.values())


class ContractError(ValueError):
    """Raised when a repository data contract is malformed or inconsistent."""


def load_contracts(contract_dir: Path) -> tuple[ContractSpec, ...]:
    """Load and structurally validate all FraudStream contract YAML files."""

    contracts = tuple(_load_contract(path) for path in sorted(contract_dir.glob("*.contract.yaml")))
    if not contracts:
        raise ContractError(f"no contract files found in {contract_dir}")

    pipeline_ids = {pipeline.pipeline_id for pipeline in PIPELINES}
    seen_contract_ids: set[str] = set()
    seen_datasets: set[str] = set()
    seen_rules: set[str] = set()
    for contract in contracts:
        if contract.contract_id in seen_contract_ids:
            raise ContractError(f"duplicate contract id: {contract.contract_id}")
        if contract.pipeline_id not in pipeline_ids:
            raise ContractError(
                f"contract {contract.contract_id} references unknown pipeline {contract.pipeline_id}"
            )
        seen_contract_ids.add(contract.contract_id)
        contract_dataset_names = {dataset.name for dataset in contract.datasets}
        for dataset_name in contract_dataset_names:
            if dataset_name not in DATASETS_BY_NAME:
                raise ContractError(f"contract references unknown dataset: {dataset_name}")
            if dataset_name in seen_datasets:
                raise ContractError(f"dataset is covered by multiple contracts: {dataset_name}")
            seen_datasets.add(dataset_name)
        for rule in contract.rules:
            if rule.rule_id in seen_rules:
                raise ContractError(f"duplicate quality rule id: {rule.rule_id}")
            if rule.target not in contract_dataset_names:
                raise ContractError(
                    f"rule {rule.rule_id} targets a dataset outside contract {contract.contract_id}"
                )
            if rule.evaluator not in EVALUATORS:
                raise ContractError(
                    f"rule {rule.rule_id} uses unknown evaluator {rule.evaluator}"
                )
            seen_rules.add(rule.rule_id)
    return contracts


def _load_contract(path: Path) -> ContractSpec:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ContractError(f"cannot read contract {path}: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise ContractError(f"contract must be a YAML object: {path}")
    if payload.get("contractFormat") != "fraudstream-data-contract/v1":
        raise ContractError(f"unsupported contract format: {path}")

    metadata = _mapping(payload, "metadata", path)
    datasets_payload = _list(payload, "datasets", path)
    rules_payload = _list(payload, "qualityRules", path)
    datasets = tuple(
        ContractDataset(
            name=_string(item, "name", path),
            grain=_string(item, "grain", path),
            required_fields=tuple(_string_list(item, "requiredFields", path)),
        )
        for item in (_item_mapping(item, path) for item in datasets_payload)
    )
    rules = tuple(
        ContractRule(
            rule_id=_string(item, "id", path),
            target=_string(item, "target", path),
            description=_string(item, "description", path),
            evaluator=_string(item, "evaluator", path),
            severity=_severity(item.get("severity", "HIGH"), path),
        )
        for item in (_item_mapping(item, path) for item in rules_payload)
    )
    if not datasets or not rules:
        raise ContractError(f"contract requires datasets and qualityRules: {path}")
    return ContractSpec(
        contract_id=_string(metadata, "id", path),
        version=_string(metadata, "version", path),
        status=_string(metadata, "status", path),
        owner=_string(metadata, "owner", path),
        pipeline_id=_string(metadata, "pipeline", path),
        source_path=path,
        datasets=datasets,
        rules=rules,
    )


def evaluate_contracts(
    project_root: Path,
    contracts: tuple[ContractSpec, ...],
) -> tuple[ValidationResult, ...]:
    """Evaluate contract rules from the same artifacts used by Airflow gates."""

    results: list[ValidationResult] = []
    for contract in contracts:
        for rule in contract.rules:
            try:
                passed, message, properties = EVALUATORS[rule.evaluator](project_root)
            except (ContractError, OSError, json.JSONDecodeError, KeyError, TypeError) as exc:
                results.append(
                    ValidationResult(
                        rule=rule,
                        status="ERROR",
                        message=str(exc),
                        properties={"error": str(exc)},
                    )
                )
                continue
            results.append(
                ValidationResult(
                    rule=rule,
                    status="SUCCESS" if passed else "FAILURE",
                    message=message,
                    properties={key: str(value) for key, value in properties.items()},
                )
            )
    return tuple(results)


def contract_index(
    contracts: tuple[ContractSpec, ...],
) -> dict[str, tuple[ContractSpec, ContractDataset]]:
    return {
        dataset.name: (contract, dataset)
        for contract in contracts
        for dataset in contract.datasets
    }


def validate_contract_schema(project_root: Path, contracts: tuple[ContractSpec, ...]) -> None:
    """Fail publishing when a contract requires a column absent from PostgreSQL DDL.

    Only the fraudstream database has DDL in the repository. The RAG tables are
    made by Spark and Feast, and their columns are checked by the RAG tests.
    """

    tables = [
        (contract, dataset)
        for contract in contracts
        for dataset in contract.datasets
        if DATASETS_BY_NAME[dataset.name].platform == POSTGRES_PLATFORM
        and dataset.name.startswith(f"{DATABASE_NAME}.")
    ]
    if not tables:
        return
    schema_path = project_root / "infra/postgres/init/001_create_fraudstream_schema.sql"
    if not schema_path.is_file():
        raise ContractError(f"PostgreSQL schema is missing: {schema_path}")
    sql = schema_path.read_text(encoding="utf-8")
    for contract, dataset in tables:
        _, schema, table = dataset.name.split(".", maxsplit=2)
        marker = f"CREATE TABLE IF NOT EXISTS {schema}.{table} ("
        if marker not in sql:
            raise ContractError(f"contract table is absent from PostgreSQL DDL: {dataset.name}")
        table_block = sql.split(marker, maxsplit=1)[1].split("\nCREATE ", maxsplit=1)[0]
        columns = set(
            re.findall(r"^\s{4}([A-Za-z_][A-Za-z0-9_]*)\s+", table_block, re.MULTILINE)
        )
        missing_fields = sorted(set(dataset.required_fields) - columns)
        if missing_fields:
            raise ContractError(
                f"contract {contract.contract_id} requires missing fields on "
                f"{dataset.name}: {', '.join(missing_fields)}"
            )


def result_index(results: tuple[ValidationResult, ...]) -> dict[str, tuple[ValidationResult, ...]]:
    targets = {result.rule.target for result in results}
    return {
        target: tuple(result for result in results if result.rule.target == target)
        for target in targets
    }


def dataset_validation_status(results: tuple[ValidationResult, ...]) -> str:
    if not results:
        return "NOT_EVALUATED"
    statuses = {result.status for result in results}
    if "ERROR" in statuses:
        return "ERROR"
    if "FAILURE" in statuses:
        return "FAILURE"
    return "SUCCESS"


def _read_json(project_root: Path, relative_path: str) -> Mapping[str, Any]:
    path = project_root / relative_path
    if not path.is_file():
        raise ContractError(f"required validation artifact is missing: {relative_path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ContractError(f"validation artifact must be an object: {relative_path}")
    return payload


def _integer(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractError(f"validation field {key!r} must be an integer")
    return value


def _table_counts(payload: Mapping[str, Any]) -> dict[str, int]:
    tables = payload.get("tables")
    if not isinstance(tables, list):
        raise ContractError("validation field 'tables' must be a list")
    return {
        _string(_item_mapping(item, Path("validation artifact")), "table_name", Path("validation artifact")): _integer(
            _item_mapping(item, Path("validation artifact")), "row_count"
        )
        for item in tables
    }


def _bronze_row_reconciliation(root: Path) -> tuple[bool, str, Mapping[str, Any]]:
    report = _read_json(root, "data/bronze/raw_transactions/_bronze_validation_summary.json")
    source = _mapping(report, "source", Path("_bronze_validation_summary.json"))
    bronze = _mapping(report, "bronze", Path("_bronze_validation_summary.json"))
    source_rows = _integer(source, "row_count")
    bronze_rows = _integer(bronze, "row_count")
    passed = report.get("passed") is True and source_rows > 0 and source_rows == bronze_rows
    return passed, "Source and Bronze row counts must reconcile.", {
        "source_rows": source_rows,
        "bronze_rows": bronze_rows,
    }


def _bronze_file_coverage(root: Path) -> tuple[bool, str, Mapping[str, Any]]:
    report = _read_json(root, "data/bronze/raw_transactions/_bronze_validation_summary.json")
    source = _mapping(report, "source", Path("_bronze_validation_summary.json"))
    bronze = _mapping(report, "bronze", Path("_bronze_validation_summary.json"))
    source_files = _integer(source, "csv_file_count")
    covered_files = _integer(bronze, "distinct_source_file_count")
    passed = source_files > 0 and source_files == covered_files
    return passed, "Every source CSV must be represented in Bronze metadata.", {
        "source_files": source_files,
        "covered_files": covered_files,
    }


def _silver_row_accounting(root: Path) -> tuple[bool, str, Mapping[str, Any]]:
    summary = _read_json(root, "data/silver/transactions/_silver_transactions_summary.json")
    input_rows = _integer(summary, "input_row_count")
    selected = _integer(summary, "output_row_count")
    quarantined = _integer(summary, "quarantined_row_count")
    duplicate_rejected = _integer(summary, "duplicate_rows_removed_count")
    accounted = selected + quarantined + duplicate_rejected
    return input_rows > 0 and input_rows == accounted, "Every Silver input row must have an explicit outcome.", {
        "input_rows": input_rows,
        "selected_rows": selected,
        "quarantined_rows": quarantined,
        "duplicate_rejected_rows": duplicate_rejected,
        "accounted_rows": accounted,
    }


def _silver_status_accounting(root: Path) -> tuple[bool, str, Mapping[str, Any]]:
    summary = _read_json(root, "data/silver/transactions/_silver_transactions_summary.json")
    selected = _integer(summary, "output_row_count")
    valid = _integer(summary, "valid_row_count")
    warning = _integer(summary, "warning_row_count")
    return selected == valid + warning, "Selected Silver rows must be valid or warning-classified.", {
        "selected_rows": selected,
        "valid_rows": valid,
        "warning_rows": warning,
    }


def _gold_fact_matches_silver(root: Path) -> tuple[bool, str, Mapping[str, Any]]:
    gold = _read_json(root, "data/gold/_gold_transactions_summary.json")
    silver = _read_json(root, "data/silver/transactions/_silver_transactions_summary.json")
    fact_rows = _integer(gold, "fact_transaction_count")
    silver_rows = _integer(silver, "output_row_count")
    passed = gold.get("build_scope") == "core" and fact_rows > 0 and fact_rows == silver_rows
    return passed, "Gold transaction grain must match selected Silver transactions.", {
        "gold_fact_rows": fact_rows,
        "silver_selected_rows": silver_rows,
        "gold_build_scope": gold.get("build_scope", "missing"),
    }


def _gold_core_table_completeness(root: Path) -> tuple[bool, str, Mapping[str, Any]]:
    gold = _read_json(root, "data/gold/_gold_transactions_summary.json")
    table_counts = _table_counts(gold)
    missing = sorted(set(CORE_GOLD_TABLE_NAMES) - set(table_counts))
    empty = sorted(name for name in CORE_GOLD_TABLE_NAMES if table_counts.get(name, 0) <= 0)
    return not missing and not empty, "Every required core Gold table must exist and contain rows.", {
        "expected_table_count": len(CORE_GOLD_TABLE_NAMES),
        "reported_table_count": len(table_counts),
        "missing_tables": ",".join(missing) or "none",
        "empty_tables": ",".join(empty) or "none",
    }


def _training_row_grain(root: Path) -> tuple[bool, str, Mapping[str, Any]]:
    features = _read_json(root, "data/gold/_offline_features_summary.json")
    fact_rows = _integer(features, "source_fact_transaction_count")
    training_rows = _integer(features, "training_row_count")
    passed = (
        fact_rows > 0
        and fact_rows == training_rows
        and features.get("training_row_count_matches_fact") is True
    )
    return passed, "The training table must have exactly one row per source transaction.", {
        "source_fact_rows": fact_rows,
        "training_rows": training_rows,
    }


def _training_id_uniqueness(root: Path) -> tuple[bool, str, Mapping[str, Any]]:
    features = _read_json(root, "data/gold/_offline_features_summary.json")
    training_rows = _integer(features, "training_row_count")
    distinct_ids = _integer(features, "training_distinct_transaction_count")
    passed = (
        training_rows == distinct_ids
        and features.get("training_transaction_ids_are_unique") is True
    )
    return passed, "Training transaction IDs must be unique.", {
        "training_rows": training_rows,
        "distinct_transaction_ids": distinct_ids,
    }


def _rag_part(root: Path, part: str) -> Mapping[str, Any]:
    return _mapping(_read_json(root, RAG_SUMMARY), part, Path(RAG_SUMMARY))


def _rag_sources_present(root: Path) -> tuple[bool, str, Mapping[str, Any]]:
    validation = _rag_part(root, "validation")
    documents = _mapping(validation, "documents", Path(RAG_SUMMARY))
    problems = _list(validation, "problems", Path(RAG_SUMMARY))
    counts = {source: _integer(documents, source) for source in RAG_SOURCES}
    passed = not problems and all(count > 0 for count in counts.values())
    return passed, "Every source must give documents, and every document must be safe to index.", {
        **counts,
        "problems": len(problems),
    }


def _rag_chunks_embedded(root: Path) -> tuple[bool, str, Mapping[str, Any]]:
    embedded = _rag_part(root, "embed")
    chunks = _integer(embedded, "chunks")
    dimensions = _list(embedded, "dimensions", Path(RAG_SUMMARY))
    lowest, highest = float(embedded["min_norm"]), float(embedded["max_norm"])
    passed = chunks > 0 and dimensions == [RAG_DIMENSIONS] and abs(lowest - 1) < 1e-3 and abs(highest - 1) < 1e-3
    return passed, f"Every chunk must have a {RAG_DIMENSIONS}-dimension vector of length 1.", {
        "chunks": chunks,
        "dimensions": ",".join(str(dimension) for dimension in dimensions),
        "min_norm": lowest,
        "max_norm": highest,
    }


def _rag_stores_agree(root: Path) -> tuple[bool, str, Mapping[str, Any]]:
    chunks = _integer(_rag_part(root, "embed"), "chunks")
    offline_rows = _integer(_rag_part(root, "offline"), "rows")
    online_rows = _integer(_rag_part(root, "online"), "rows")
    passed = chunks > 0 and chunks == offline_rows == online_rows
    return passed, "The Iceberg table and the pgvector store must hold exactly the current chunks.", {
        "chunks": chunks,
        "offline_rows": offline_rows,
        "online_rows": online_rows,
    }


def _rag_golden_retrieval(root: Path) -> tuple[bool, str, Mapping[str, Any]]:
    retrieval = _rag_part(root, "retrieval")
    questions = _integer(retrieval, "questions")
    hits = _integer(retrieval, "hits")
    passed = questions > 0 and retrieval.get("passed") is True
    return passed, "Golden questions must find their document in the top results through Feast.", {
        "questions": questions,
        "hits": hits,
        "top_k": _integer(retrieval, "top_k"),
        "min_hit_rate": retrieval.get("min_hit_rate", "missing"),
    }


Evaluator = Callable[[Path], tuple[bool, str, Mapping[str, Any]]]
EVALUATORS: dict[str, Evaluator] = {
    "bronze_row_reconciliation": _bronze_row_reconciliation,
    "bronze_file_coverage": _bronze_file_coverage,
    "silver_row_accounting": _silver_row_accounting,
    "silver_status_accounting": _silver_status_accounting,
    "gold_fact_matches_silver": _gold_fact_matches_silver,
    "gold_core_table_completeness": _gold_core_table_completeness,
    "training_row_grain": _training_row_grain,
    "training_id_uniqueness": _training_id_uniqueness,
    "rag_sources_present": _rag_sources_present,
    "rag_chunks_embedded": _rag_chunks_embedded,
    "rag_stores_agree": _rag_stores_agree,
    "rag_golden_retrieval": _rag_golden_retrieval,
}


def _mapping(payload: Mapping[str, Any], key: str, path: Path) -> Mapping[str, Any]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise ContractError(f"{path}: field {key!r} must be an object")
    return value


def _list(payload: Mapping[str, Any], key: str, path: Path) -> list[Any]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise ContractError(f"{path}: field {key!r} must be a list")
    return value


def _item_mapping(value: Any, path: Path) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ContractError(f"{path}: list entries must be objects")
    return value


def _string(payload: Mapping[str, Any], key: str, path: Path) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{path}: field {key!r} must be a non-empty string")
    return value.strip()


def _string_list(payload: Mapping[str, Any], key: str, path: Path) -> list[str]:
    values = _list(payload, key, path)
    if not values or not all(isinstance(value, str) and value.strip() for value in values):
        raise ContractError(f"{path}: field {key!r} must contain non-empty strings")
    return [value.strip() for value in values]


def _severity(value: Any, path: Path) -> str:
    if value not in {"LOW", "MEDIUM", "HIGH"}:
        raise ContractError(f"{path}: severity must be LOW, MEDIUM, or HIGH")
    return str(value)
