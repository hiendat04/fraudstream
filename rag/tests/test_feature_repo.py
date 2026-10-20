"""The fraudstream_rag Feast project: its schema matches the chunk table, and its store is pgvector."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from feast.repo_config import load_repo_config

from feature_repo import definitions
from fraudstream_rag.embedding import DIMENSIONS
from fraudstream_rag.offline import CHUNK_COLUMNS

RAG_REPO = Path(__file__).resolve().parents[1] / "feature_repo"
# Found by walking up, because mutmut runs the tests from a copy one folder deeper.
MAIN_REPO = next(
    parent / "feature_store" / "feature_repo"
    for parent in Path(__file__).resolve().parents
    if (parent / "feature_store" / "feature_repo").is_dir()
)
ENV = {
    "POSTGRES_USER": "fraudstream",
    "POSTGRES_PASSWORD": "pg-secret",
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "fraudstream",
    "PGVECTOR_HOST": "pgvector",
    "PGVECTOR_PORT": "5432",
    "PGVECTOR_DB": "llm",
    "PGVECTOR_USER": "llm",
    "PGVECTOR_PASSWORD": "llm-secret",
    "REDIS_HOST": "redis",
    "REDIS_PORT": "6379",
    "FEAST_SPARK_MASTER": "local[4]",
    "FRAUDSTREAM_WAREHOUSE_URI": "s3a://fraudstream/warehouse",
    "MINIO_ENDPOINT": "http://minio:9000",
    "MINIO_ACCESS_KEY": "key",
    "MINIO_SECRET_KEY": "secret",
}


def test_the_feature_view_holds_every_chunk_column_but_the_timestamp() -> None:
    view = definitions.document_chunks

    assert view.name == "document_chunks"
    assert view.entities == ["chunk"]
    assert [field.name for field in view.entity_columns] == ["chunk_id"]
    assert [field.name for field in view.features] == [column for column in CHUNK_COLUMNS if column not in ("chunk_id", "updated_at")]
    assert view.batch_source.timestamp_field == "updated_at"


def test_the_embedding_is_a_cosine_vector_index_of_the_models_size() -> None:
    embedding = next(field for field in definitions.document_chunks.features if field.name == "embedding")

    assert (embedding.vector_index, embedding.vector_length, embedding.vector_search_metric) == (True, DIMENSIONS, "cosine")


def test_the_online_store_is_pgvector_in_its_own_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in ENV.items():
        monkeypatch.setenv(name, value)

    config = load_repo_config(RAG_REPO, RAG_REPO / "feature_store.yaml")

    assert config.project == "fraudstream_rag"
    online = config.online_store
    assert (online.type, online.host, online.port, online.database, online.db_schema) == ("postgres", "pgvector", 5432, "llm", "rag")
    assert (online.vector_enabled, online.sslmode) == (True, "disable")


def test_the_registry_and_lakehouse_settings_are_the_fraudstream_projects() -> None:
    rag = yaml.safe_load((RAG_REPO / "feature_store.yaml").read_text())
    main = yaml.safe_load((MAIN_REPO / "feature_store.yaml").read_text())

    assert rag["registry"] == main["registry"]
    differing = {
        key for key in rag["offline_store"]["spark_conf"].keys() | main["offline_store"]["spark_conf"].keys()
        if rag["offline_store"]["spark_conf"].get(key) != main["offline_store"]["spark_conf"].get(key)
    }
    assert differing == {"spark.app.name"}


def test_the_datahub_contract_names_only_chunk_columns() -> None:
    contract = yaml.safe_load((MAIN_REPO.parents[1] / "datahub" / "contracts" / "rag_index.contract.yaml").read_text())

    for dataset in contract["datasets"]:
        assert set(dataset["requiredFields"]) <= set(CHUNK_COLUMNS), dataset["name"]
