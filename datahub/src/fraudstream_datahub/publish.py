"""Publish FraudStream pipelines, lineage, contracts, and validation to DataHub."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Sequence

from fraudstream_datahub.model import (
    DATASETS_BY_NAME,
    PIPELINES,
    ContractSpec,
    DatasetRef,
    ValidationResult,
    contract_index,
    dataset_validation_status,
    evaluate_contracts,
    load_contracts,
    result_index,
    validate_contract_schema,
)


DEFAULT_SERVER = "http://localhost:18082"
DEFAULT_UI_URL = "http://localhost:9002"
DEFAULT_ENV = "PROD"
DEFAULT_PLATFORM_INSTANCE = "fraudstream-local"


def build_report(
    project_root: Path,
    contract_dir: Path,
    env: str,
    platform_instance: str = DEFAULT_PLATFORM_INSTANCE,
) -> tuple[tuple[ContractSpec, ...], tuple[ValidationResult, ...], dict[str, Any]]:
    """Build and validate all metadata before touching DataHub."""

    contracts = load_contracts(contract_dir)
    validate_contract_schema(project_root, contracts)
    results = evaluate_contracts(project_root, contracts)
    statuses = {status: 0 for status in ("SUCCESS", "FAILURE", "ERROR")}
    for result in results:
        statuses[result.status] += 1
    report = {
        "pipelines": [pipeline.pipeline_id for pipeline in PIPELINES],
        "dataset_count": len(DATASETS_BY_NAME),
        "contract_count": len(contracts),
        "contract_schema_check": "SUCCESS",
        "assertion_count": len(results),
        "assertion_status_counts": statuses,
        "dataset_urns": sorted(
            dataset.urn(env, platform_instance) for dataset in DATASETS_BY_NAME.values()
        ),
    }
    return contracts, results, report


def publish(
    *,
    project_root: Path,
    contract_dir: Path,
    server: str,
    token: str | None,
    ui_url: str,
    env: str,
    platform_instance: str,
) -> dict[str, Any]:
    """Upsert the complete governance graph and measured assertion results."""

    from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
    from datahub.configuration.common import GraphError
    from datahub.sdk import DataFlow, DataHubClient, DataJob, Dataset

    contracts, results, report = build_report(
        project_root, contract_dir, env, platform_instance
    )
    contracts_by_dataset = contract_index(contracts)
    results_by_dataset = result_index(results)

    graph = DataHubGraph(
        config=DatahubClientConfig(server=server.rstrip("/"), token=token or None)
    )
    client = DataHubClient(graph=graph)
    client.test_connection()

    for dataset in DATASETS_BY_NAME.values():
        contract_entry = contracts_by_dataset.get(dataset.name)
        dataset_results = results_by_dataset.get(dataset.name, ())
        properties = _dataset_properties(
            dataset=dataset,
            contract_entry=contract_entry,
            results=dataset_results,
            project_root=project_root,
        )
        entity = Dataset(
            platform=dataset.platform,
            # Dataset SDK applies platform_instance to the URN automatically.
            name=dataset.name,
            platform_instance=platform_instance if dataset.platform == "postgres" else None,
            env=env,
            display_name=dataset.name,
            description=dataset.description,
            custom_properties=properties,
        )
        client.entities.upsert(entity)

    for pipeline in PIPELINES:
        airflow_url = f"{ui_url.rstrip('/')}/dags/{pipeline.dag_id}"
        flow = DataFlow(
            name=pipeline.pipeline_id,
            platform="airflow",
            platform_instance=platform_instance,
            env=env,
            display_name=pipeline.title,
            description=pipeline.description,
            external_url=airflow_url,
            custom_properties={
                "airflow.dag_id": pipeline.dag_id,
                "governance.contract": _contract_id_for_pipeline(contracts, pipeline.pipeline_id),
                "governance.validation": _pipeline_status(results, contracts, pipeline.pipeline_id),
            },
        )
        job = DataJob(
            name=pipeline.dag_id,
            flow=flow,
            display_name=pipeline.dag_id,
            description=pipeline.description,
            external_url=airflow_url,
            custom_properties={
                "pipeline.id": pipeline.pipeline_id,
                "stage.model": "ingest -> validate",
            },
            inlets=[dataset.urn(env, platform_instance) for dataset in pipeline.inputs],
            outlets=[dataset.urn(env, platform_instance) for dataset in pipeline.outputs],
        )
        client.entities.upsert(flow)
        client.entities.upsert(job)
        for upstream, downstream in pipeline.lineage:
            client.lineage.add_lineage(
                upstream=upstream.urn(env, platform_instance),
                downstream=downstream.urn(env, platform_instance),
            )

    timestamp_millis = int(time.time() * 1000)
    assertions_to_report: list[tuple[str, ValidationResult]] = []
    for result in results:
        assertion_urn = f"urn:li:assertion:fraudstream-{result.rule.rule_id}"
        graph.upsert_custom_assertion(
            urn=assertion_urn,
            entity_urn=DATASETS_BY_NAME[result.rule.target].urn(env, platform_instance),
            type="FraudStream Data Contract",
            description=result.rule.description,
            platform_urn="urn:li:dataPlatform:airflow",
            logic=result.rule.evaluator,
        )
        assertions_to_report.append((assertion_urn, result))

    for assertion_urn, result in assertions_to_report:
        properties = [
            {"key": "message", "value": result.message},
            *(
                {"key": key, "value": value}
                for key, value in sorted(result.properties.items())
            ),
        ]
        for attempt in range(10):
            try:
                graph.report_assertion_result(
                    urn=assertion_urn,
                    timestamp_millis=timestamp_millis,
                    type=result.status,
                    properties=properties,
                    severity=result.rule.severity,
                    error_type="UNKNOWN_ERROR" if result.status == "ERROR" else None,
                    error_message=result.message if result.status == "ERROR" else None,
                )
                break
            except GraphError as exc:
                transient = "does not exist or is not associated with any entity" in str(exc)
                if not transient or attempt == 9:
                    raise
                time.sleep(1)

    report["datahub_server"] = server
    report["published"] = True
    return report


def _dataset_properties(
    *,
    dataset: DatasetRef,
    contract_entry: tuple[ContractSpec, Any] | None,
    results: tuple[ValidationResult, ...],
    project_root: Path,
) -> dict[str, str]:
    properties = {
        "governance.layer": dataset.layer,
        "governance.owner": "fraudstream-data-platform",
    }
    if contract_entry is None:
        properties["contract.status"] = "NOT_APPLICABLE"
        return properties

    contract, contract_dataset = contract_entry
    source_path = contract.source_path
    try:
        source_path = source_path.relative_to(project_root)
    except ValueError:
        pass
    properties.update(
        {
            "governance.pipeline": contract.pipeline_id,
            "contract.id": contract.contract_id,
            "contract.version": contract.version,
            "contract.status": contract.status.upper(),
            "contract.owner": contract.owner,
            "contract.source": str(source_path),
            "contract.grain": contract_dataset.grain,
            "contract.required_fields": ", ".join(contract_dataset.required_fields),
            "contract.schema_check": "SUCCESS",
            "validation.status": dataset_validation_status(results),
            "validation.assertion_count": str(len(results)),
            "validation.passed_count": str(
                sum(result.status == "SUCCESS" for result in results)
            ),
        }
    )
    return properties


def _contract_id_for_pipeline(
    contracts: tuple[ContractSpec, ...], pipeline_id: str
) -> str:
    return next(contract.contract_id for contract in contracts if contract.pipeline_id == pipeline_id)


def _pipeline_status(
    results: tuple[ValidationResult, ...],
    contracts: tuple[ContractSpec, ...],
    pipeline_id: str,
) -> str:
    contract = next(contract for contract in contracts if contract.pipeline_id == pipeline_id)
    rule_ids = {rule.rule_id for rule in contract.rules}
    return dataset_validation_status(
        tuple(result for result in results if result.rule.rule_id in rule_ids)
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish FraudStream pipeline governance metadata to DataHub."
    )
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--contract-dir", type=Path)
    parser.add_argument("--server", default=DEFAULT_SERVER)
    parser.add_argument("--token")
    parser.add_argument("--ui-url", default=DEFAULT_UI_URL)
    parser.add_argument("--env", default=DEFAULT_ENV)
    parser.add_argument("--platform-instance", default=DEFAULT_PLATFORM_INSTANCE)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate contracts and quality artifacts without connecting to DataHub.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    project_root = args.project_root.expanduser().resolve()
    contract_dir = (
        args.contract_dir.expanduser().resolve()
        if args.contract_dir
        else project_root / "datahub" / "contracts"
    )
    if args.dry_run:
        _, _, report = build_report(
            project_root, contract_dir, args.env, args.platform_instance
        )
    else:
        report = publish(
            project_root=project_root,
            contract_dir=contract_dir,
            server=args.server,
            token=args.token,
            ui_url=args.ui_url,
            env=args.env,
            platform_instance=args.platform_instance,
        )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
