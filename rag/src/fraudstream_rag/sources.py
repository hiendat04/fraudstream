"""Read the three sources, and copy the handbook from Git to MinIO."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx

from fraudstream import storage
from fraudstream.jobs.warehouse import WarehouseConfig
from fraudstream_rag.chunking import Document
from fraudstream_rag.documents import feature_view_page, handbook_page, model_card

MODEL = "fraud-detection"


def handbook_documents(warehouse: WarehouseConfig, uri: str) -> list[Document]:
    return [
        handbook_page(key.rsplit("/", 1)[-1], storage.get_text(warehouse, key))
        for key in storage.list_keys(warehouse, uri.rstrip("/") + "/", suffix=".md")
    ]


def model_card_documents(mlflow: httpx.Client, model: str = MODEL) -> list[Document]:
    """A card per registered version, from MLflow's REST API: versions, aliases, and each version's run."""

    def get(path: str, **params: Any) -> dict[str, Any]:
        response = mlflow.get(f"/api/2.0/mlflow/{path}", params=params)
        response.raise_for_status()
        return response.json()

    aliases: dict[str, list[str]] = {}
    for alias in get("registered-models/get", name=model)["registered_model"].get("aliases", []):
        aliases.setdefault(alias["version"], []).append(alias["alias"])
    versions = get("model-versions/search", filter=f"name='{model}'", max_results=1000).get("model_versions", [])
    newest = max((int(version["version"]) for version in versions), default=0)
    cards = []
    for version in versions:
        run = get("runs/get", run_id=version["run_id"])["run"]
        cards.append(
            model_card(
                model,
                version["version"],
                aliases=aliases.get(version["version"], []),
                newest=int(version["version"]) == newest,
                created_ms=int(version["creation_timestamp"]),
                run={
                    "run_id": run["info"]["run_id"],
                    "metrics": {item["key"]: item["value"] for item in run["data"].get("metrics", [])},
                    "params": {item["key"]: item["value"] for item in run["data"].get("params", [])},
                },
            )
        )
    return sorted(cards, key=lambda card: card.doc_id)


def feature_dictionary_documents(feature_store: Any) -> list[Document]:
    descriptions = {entity.name: entity.description for entity in feature_store.list_entities()}
    return [
        feature_view_page(view, descriptions)
        for view in sorted(feature_store.list_feature_views(), key=lambda view: view.name)
    ]


def sync_handbook(warehouse: WarehouseConfig, source: Path, target: str) -> dict[str, Any]:
    """Make the MinIO prefix hold exactly the Markdown files in source: upload what differs, delete what is gone."""

    local = {path.name: path.read_text(encoding="utf-8") for path in sorted(source.glob("*.md"))}
    if not local:
        raise ValueError(f"no .md files in {source}: refusing to empty {target}")
    prefix = target.rstrip("/") + "/"
    remote = {key.rsplit("/", 1)[-1]: key for key in storage.list_keys(warehouse, prefix, suffix=".md")}
    uploaded = [
        name for name, text in local.items()
        if name not in remote or storage.get_text(warehouse, remote[name]) != text
    ]
    for name in uploaded:
        storage.put_text(warehouse, storage.join_uri(target, name), local[name])
    deleted = sorted(set(remote) - set(local))
    for name in deleted:
        storage.delete_object(warehouse, remote[name])
    return {"uploaded": uploaded, "deleted": deleted, "unchanged": len(local) - len(uploaded)}
