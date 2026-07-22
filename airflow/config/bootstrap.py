"""Render environment-backed Airflow bootstrap files for the initialization CLI."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


CONFIG_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = Path("/tmp/fraudstream-airflow-bootstrap")


def _load_json(file_name: str) -> dict[str, Any]:
    with (CONFIG_DIR / file_name).open(encoding="utf-8") as file:
        payload = json.load(file)
    if not isinstance(payload, dict):
        raise ValueError(f"{file_name} must contain a JSON object")
    return payload


def _expand_environment(value: Any) -> Any:
    if isinstance(value, str):
        expanded = os.path.expandvars(value)
        if "${" in expanded:
            raise ValueError(f"required environment variable is not set: {value}")
        return expanded
    if isinstance(value, list):
        return [_expand_environment(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand_environment(item) for key, item in value.items()}
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)


def main() -> None:
    """Write expanded connections and the repository-owned Spark pool definition."""

    variables = _load_json("variables.json")
    slots = int(variables["fraudstream_spark_pool_slots"])
    if slots <= 0:
        raise ValueError("fraudstream_spark_pool_slots must be positive")

    _write_json(
        OUTPUT_DIR / "connections.json",
        _expand_environment(_load_json("connections.json")),
    )
    _write_json(
        OUTPUT_DIR / "pools.json",
        {
            "fraudstream_spark": {
                "slots": slots,
                "description": "Serializes local FraudStream Spark jobs",
                "include_deferred": False,
            }
        },
    )
    print(f"Rendered Airflow bootstrap files in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
