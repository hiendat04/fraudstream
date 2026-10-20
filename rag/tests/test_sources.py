"""The sources: the handbook in object storage, MLflow's registry over REST, the Feast registry."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from fraudstream.jobs.warehouse import WarehouseConfig
from fraudstream_rag.sources import (
    feature_dictionary_documents,
    handbook_documents,
    model_card_documents,
    sync_handbook,
)

WAREHOUSE = WarehouseConfig()


def run(run_id: str, threshold: float) -> dict:
    return {
        "run": {
            "info": {"run_id": run_id},
            "data": {
                "metrics": [{"key": "threshold", "value": threshold}, {"key": "pr_auc", "value": 0.03}],
                "params": [{"key": "data_snapshot_id", "value": "141"}],
            },
        }
    }


MLFLOW = {
    "/api/2.0/mlflow/registered-models/get": {"registered_model": {"name": "fraud-detection", "aliases": [{"alias": "production", "version": "4"}]}},
    "/api/2.0/mlflow/model-versions/search": {
        "model_versions": [
            {"version": "5", "run_id": "r5", "creation_timestamp": 1791998599413},
            {"version": "4", "run_id": "r4", "creation_timestamp": 1791900000000},
        ]
    },
}


def mlflow_client(status: int = 200) -> tuple[httpx.Client, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if status != 200:
            return httpx.Response(status)
        if request.url.path == "/api/2.0/mlflow/runs/get":
            run_id = request.url.params["run_id"]
            return httpx.Response(200, json=run(run_id, 0.6 if run_id == "r4" else 0.7))
        return httpx.Response(200, json=MLFLOW[request.url.path])

    return httpx.Client(base_url="http://mlflow:5000", transport=httpx.MockTransport(handle)), seen


def test_every_registered_version_gets_a_card_marked_with_what_it_serves() -> None:
    client, seen = mlflow_client()

    cards = model_card_documents(client)

    assert [card.doc_id for card in cards] == ["model-cards/fraud-detection-v4", "model-cards/fraud-detection-v5"]
    assert "(production)" in cards[0].text and "at or above 0.6000" in cards[0].text
    assert "(newest)" in cards[1].text and "at or above 0.7000" in cards[1].text
    search = next(request for request in seen if request.url.path.endswith("model-versions/search"))
    assert dict(search.url.params) == {"filter": "name='fraud-detection'", "max_results": "1000"}
    registered = next(request for request in seen if request.url.path.endswith("registered-models/get"))
    assert dict(registered.url.params) == {"name": "fraud-detection"}


def test_a_run_that_logged_nothing_still_gets_a_card() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("runs/get"):
            return httpx.Response(200, json={"run": {"info": {"run_id": "r1"}, "data": {}}})
        if request.url.path.endswith("model-versions/search"):
            return httpx.Response(200, json={"model_versions": [{"version": "1", "run_id": "r1", "creation_timestamp": 0}]})
        return httpx.Response(200, json={"registered_model": {}})

    [card] = model_card_documents(httpx.Client(base_url="http://mlflow:5000", transport=httpx.MockTransport(handle)))

    assert "(newest)" in card.text and "at or above not recorded" in card.text and "run r1" in card.text


def test_mlflow_failing_stops_the_load() -> None:
    client, _ = mlflow_client(status=503)

    with pytest.raises(httpx.HTTPStatusError):
        model_card_documents(client)


def test_a_model_with_no_versions_gives_no_cards() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("registered-models/get"):
            return httpx.Response(200, json={"registered_model": {"name": "fraud-detection"}})
        return httpx.Response(200, json={})

    client = httpx.Client(base_url="http://mlflow:5000", transport=httpx.MockTransport(handle))

    assert model_card_documents(client) == []


def test_the_handbook_is_every_markdown_file_under_the_prefix(tmp_path: Path) -> None:
    (tmp_path / "b-policy.md").write_text("# B\n\nb\n")
    (tmp_path / "a-policy.md").write_text("# A\n\na\n")
    (tmp_path / "notes.txt").write_text("not a page")

    pages = handbook_documents(WAREHOUSE, f"file://{tmp_path}")

    assert [(page.doc_id, page.text) for page in pages] == [("handbook/a-policy", "# A\n\na\n"), ("handbook/b-policy", "# B\n\nb\n")]


def test_the_feature_dictionary_is_a_page_per_view_in_name_order() -> None:
    def view(name: str) -> SimpleNamespace:
        return SimpleNamespace(
            name=name, entities=["merchant"], features=[], ttl=None, tags={},
            stream_source=None, batch_source=SimpleNamespace(name="src", description="d"),
        )

    store = SimpleNamespace(
        list_entities=lambda: [SimpleNamespace(name="merchant", description="A generated merchant.")],
        list_feature_views=lambda: [view("z_view"), view("a_view")],
    )

    pages = feature_dictionary_documents(store)

    assert [page.doc_id for page in pages] == ["feature-dictionary/a_view", "feature-dictionary/z_view"]
    assert "merchant (A generated merchant.)" in pages[0].text


def test_syncing_mirrors_the_folder_into_the_prefix(tmp_path: Path) -> None:
    source, target = tmp_path / "git", tmp_path / "minio"
    source.mkdir()
    (source / "policy.md").write_text("# Policy\n")
    (source / "rings.md").write_text("# Rings\n")
    uri = f"file://{target}"

    assert sync_handbook(WAREHOUSE, source, uri) == {"uploaded": ["policy.md", "rings.md"], "deleted": [], "unchanged": 0}
    assert sync_handbook(WAREHOUSE, source, uri) == {"uploaded": [], "deleted": [], "unchanged": 2}

    (source / "policy.md").write_text("# Policy v2\n")
    (source / "rings.md").unlink()
    (source / "contact.md").write_text("# Contact\n")

    assert sync_handbook(WAREHOUSE, source, uri) == {"uploaded": ["contact.md", "policy.md"], "deleted": ["rings.md"], "unchanged": 0}
    assert sorted(path.name for path in target.iterdir()) == ["contact.md", "policy.md"]
    assert (target / "policy.md").read_text() == "# Policy v2\n"


def test_syncing_an_empty_folder_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="refusing to empty"):
        sync_handbook(WAREHOUSE, tmp_path, f"file://{tmp_path}/minio")
