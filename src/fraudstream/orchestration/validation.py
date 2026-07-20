"""Small, scheduler-independent quality gates for FraudStream pipelines."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from fraudstream.jobs.gold.offline_features import SUMMARY_FILE_NAME as FEATURE_SUMMARY_FILE_NAME
from fraudstream.jobs.gold.transactions import (
    CORE_GOLD_TABLE_NAMES,
    OFFLINE_FEATURE_TABLE_NAMES,
    SUMMARY_FILE_NAME as GOLD_SUMMARY_FILE_NAME,
)
from fraudstream.jobs.silver.transactions import SUMMARY_FILE_NAME as SILVER_SUMMARY_FILE_NAME


class PipelineValidationError(ValueError):
    """Raised when an orchestration quality gate rejects an artifact."""


def validate_source_manifest(
    source_dir: str | Path,
    project_root: str | Path,
) -> dict[str, int]:
    """Verify the generated source manifest, files, and row-count evidence."""

    source_path = Path(source_dir)
    root_path = Path(project_root)
    manifest = _load_json(source_path / "_manifest.json")
    quality_summary = _load_json(source_path / "_quality_summary.json")
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise PipelineValidationError("source manifest must contain at least one file")

    missing_files: list[str] = []
    empty_files: list[str] = []
    for raw_path in files:
        if not isinstance(raw_path, str) or not raw_path.strip():
            raise PipelineValidationError("source manifest contains an invalid file path")
        file_path = Path(raw_path)
        if not file_path.is_absolute():
            file_path = root_path / file_path
        if not file_path.is_file():
            missing_files.append(str(file_path))
        elif file_path.stat().st_size == 0:
            empty_files.append(str(file_path))

    if missing_files:
        raise PipelineValidationError(
            f"source manifest references {len(missing_files)} missing files; first={missing_files[0]}"
        )
    if empty_files:
        raise PipelineValidationError(
            f"source manifest references {len(empty_files)} empty files; first={empty_files[0]}"
        )

    written_file_count = _required_int(quality_summary, "written_file_count")
    row_count = _required_int(quality_summary, "row_count_after_duplicates")
    if written_file_count != len(files):
        raise PipelineValidationError(
            "source file count mismatch: "
            f"manifest={len(files)}, quality_summary={written_file_count}"
        )
    if row_count <= 0:
        raise PipelineValidationError("generated source row count must be positive")

    return {"file_count": len(files), "row_count": row_count}


def validate_bronze_report(report_path: str | Path) -> dict[str, int]:
    """Reject a Bronze reconciliation report unless every core count agrees."""

    report = _load_json(Path(report_path))
    if report.get("passed") is not True:
        raise PipelineValidationError("Bronze reconciliation report did not pass")

    source = _required_mapping(report, "source")
    bronze = _required_mapping(report, "bronze")
    source_rows = _required_int(source, "row_count")
    bronze_rows = _required_int(bronze, "row_count")
    source_files = _required_int(source, "csv_file_count")
    covered_files = _required_int(bronze, "distinct_source_file_count")
    if source_rows <= 0 or source_rows != bronze_rows:
        raise PipelineValidationError(
            f"Bronze row reconciliation failed: source={source_rows}, bronze={bronze_rows}"
        )
    if source_files <= 0 or source_files != covered_files:
        raise PipelineValidationError(
            "Bronze source-file coverage failed: "
            f"source={source_files}, covered={covered_files}"
        )
    return {"source_rows": source_rows, "bronze_rows": bronze_rows}


def validate_silver_summary(
    summary_path: str | Path,
    quality_report_path: str | Path,
) -> dict[str, int]:
    """Check Silver row accounting and the presence of quality evidence."""

    summary = _load_json(Path(summary_path))
    quality_report = _load_json(Path(quality_report_path))
    input_rows = _required_int(summary, "input_row_count")
    output_rows = _required_int(summary, "output_row_count")
    quarantined_rows = _required_int(summary, "quarantined_row_count")
    duplicate_rows_removed = _required_int(summary, "duplicate_rows_removed_count")
    warning_rows = _required_int(summary, "warning_row_count")
    valid_rows = _required_int(summary, "valid_row_count")

    accounted_rows = output_rows + quarantined_rows + duplicate_rows_removed
    if input_rows <= 0 or input_rows != accounted_rows:
        raise PipelineValidationError(
            "Silver row accounting failed: "
            f"input={input_rows}, selected={output_rows}, quarantined={quarantined_rows}, "
            f"duplicate_rejected={duplicate_rows_removed}"
        )
    if output_rows != warning_rows + valid_rows:
        raise PipelineValidationError(
            "Silver selected-row status accounting failed: "
            f"selected={output_rows}, warning={warning_rows}, valid={valid_rows}"
        )

    report_counts = _required_mapping(quality_report, "row_counts")
    if _required_int(report_counts, "input") != input_rows:
        raise PipelineValidationError("Silver summary and quality report input counts differ")
    if _required_int(report_counts, "silver_output") != output_rows:
        raise PipelineValidationError("Silver summary and quality report output counts differ")
    return {"input_rows": input_rows, "output_rows": output_rows}


def validate_core_gold_summary(
    gold_dir: str | Path,
    silver_summary_path: str | Path,
    *,
    expected_scope: str = "core",
) -> dict[str, int]:
    """Verify required core Gold tables and their transaction grain."""

    gold_path = Path(gold_dir)
    summary = _load_json(gold_path / GOLD_SUMMARY_FILE_NAME)
    silver_summary = _load_json(Path(silver_summary_path))
    if summary.get("build_scope") != expected_scope:
        raise PipelineValidationError(
            f"Gold build scope must be {expected_scope!r}, got {summary.get('build_scope')!r}"
        )

    table_counts = _table_counts(summary)
    missing_tables = sorted(set(CORE_GOLD_TABLE_NAMES) - set(table_counts))
    if missing_tables:
        raise PipelineValidationError(
            f"Gold summary is missing core tables: {', '.join(missing_tables)}"
        )
    _validate_parquet_outputs(gold_path, CORE_GOLD_TABLE_NAMES)

    fact_rows = table_counts["fact_transactions"]
    silver_rows = _required_int(silver_summary, "output_row_count")
    if fact_rows <= 0 or fact_rows != silver_rows:
        raise PipelineValidationError(
            f"Gold transaction grain failed: fact={fact_rows}, silver={silver_rows}"
        )
    if _required_int(summary, "fact_transaction_count") != fact_rows:
        raise PipelineValidationError("Gold fact count does not match its table summary")
    return {"silver_rows": silver_rows, "fact_transaction_rows": fact_rows}


def validate_offline_feature_summary(gold_dir: str | Path) -> dict[str, int]:
    """Verify feature outputs and one-training-row-per-transaction invariants."""

    gold_path = Path(gold_dir)
    feature_summary = _load_json(gold_path / FEATURE_SUMMARY_FILE_NAME)
    gold_summary = _load_json(gold_path / GOLD_SUMMARY_FILE_NAME)
    table_counts = _table_counts(feature_summary)
    missing_tables = sorted(set(OFFLINE_FEATURE_TABLE_NAMES) - set(table_counts))
    if missing_tables:
        raise PipelineValidationError(
            f"offline feature summary is missing tables: {', '.join(missing_tables)}"
        )
    _validate_parquet_outputs(gold_path, OFFLINE_FEATURE_TABLE_NAMES)

    source_fact_rows = _required_int(feature_summary, "source_fact_transaction_count")
    core_fact_rows = _required_int(gold_summary, "fact_transaction_count")
    training_rows = _required_int(feature_summary, "training_row_count")
    distinct_training_ids = _required_int(
        feature_summary,
        "training_distinct_transaction_count",
    )
    if source_fact_rows <= 0 or source_fact_rows != core_fact_rows:
        raise PipelineValidationError(
            "feature source count differs from validated Gold: "
            f"feature_source={source_fact_rows}, gold_fact={core_fact_rows}"
        )
    if training_rows != source_fact_rows:
        raise PipelineValidationError(
            f"training row grain failed: training={training_rows}, fact={source_fact_rows}"
        )
    if distinct_training_ids != training_rows:
        raise PipelineValidationError(
            "training transaction IDs are not unique: "
            f"rows={training_rows}, distinct_ids={distinct_training_ids}"
        )
    if feature_summary.get("training_row_count_matches_fact") is not True:
        raise PipelineValidationError("feature summary row-count check is false")
    if feature_summary.get("training_transaction_ids_are_unique") is not True:
        raise PipelineValidationError("feature summary uniqueness check is false")
    return {
        "fact_transaction_rows": source_fact_rows,
        "training_rows": training_rows,
    }


def silver_summary_path(silver_dir: str | Path) -> Path:
    """Return the conventional Silver summary path."""

    return Path(silver_dir) / SILVER_SUMMARY_FILE_NAME


def gold_summary_path(gold_dir: str | Path) -> Path:
    """Return the conventional Gold summary path."""

    return Path(gold_dir) / GOLD_SUMMARY_FILE_NAME


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise PipelineValidationError(f"required validation artifact is missing: {path}")
    try:
        with path.open(encoding="utf-8") as file:
            payload = json.load(file)
    except (json.JSONDecodeError, OSError) as exc:
        raise PipelineValidationError(f"cannot read validation artifact {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise PipelineValidationError(f"validation artifact must be a JSON object: {path}")
    return payload


def _required_mapping(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise PipelineValidationError(f"validation artifact field {key!r} must be an object")
    return value


def _required_int(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise PipelineValidationError(f"validation artifact field {key!r} must be an integer")
    if value < 0:
        raise PipelineValidationError(f"validation artifact field {key!r} cannot be negative")
    return value


def _table_counts(summary: Mapping[str, Any]) -> dict[str, int]:
    tables = summary.get("tables")
    if not isinstance(tables, list):
        raise PipelineValidationError("validation artifact field 'tables' must be a list")
    counts: dict[str, int] = {}
    for table in tables:
        if not isinstance(table, Mapping) or not isinstance(table.get("table_name"), str):
            raise PipelineValidationError("table summary entries must contain table_name")
        table_name = table["table_name"]
        if table_name in counts:
            raise PipelineValidationError(f"duplicate table summary entry: {table_name}")
        counts[table_name] = _required_int(table, "row_count")
    return counts


def _validate_parquet_outputs(root: Path, table_names: tuple[str, ...]) -> None:
    missing_outputs = [
        table_name
        for table_name in table_names
        if not any((root / table_name).rglob("*.parquet"))
    ]
    if missing_outputs:
        raise PipelineValidationError(
            f"Parquet output is missing for tables: {', '.join(missing_outputs)}"
        )
