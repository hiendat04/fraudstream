"""The online writer: Feast writes the changed chunks, SQL lists and deletes, and nothing else is touched.

pgvector is not in the unit tests; the connection is a fake that records the SQL it
is given. The DAG's check step proves the real store holds exactly the chunks.
"""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.protos.feast.types.EntityKey_pb2 import EntityKey
from feast.protos.feast.types.Value_pb2 import Value

from fraudstream_rag import online
from fraudstream_rag.offline import CHUNK_COLUMNS


def key(chunk_id: str) -> bytes:
    return serialize_entity_key(
        EntityKey(join_keys=["chunk_id"], entity_values=[Value(string_val=chunk_id)]),
        entity_key_serialization_version=3,
    )


class FakeCursor:
    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, query: object, params: tuple = ()) -> None:
        self.connection.executed.append((repr(query), params))

    def fetchall(self) -> list[tuple]:
        return self.connection.rows


class FakeConnection:
    def __init__(self, rows: list[tuple] | None = None) -> None:
        self.rows = rows or []
        self.executed: list[tuple[str, tuple]] = []
        self.commits = 0

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1


def test_stored_hashes_reads_each_chunk_id_and_hash() -> None:
    connection = FakeConnection([(memoryview(key("a")), "h1"), (key("b"), "h2")])

    assert online.stored_hashes(connection, "rag", "fraudstream_rag_document_chunks", 3) == {"a": "h1", "b": "h2"}
    query = connection.executed[0][0]
    assert "content_hash" in query and "'rag'" in query and "'fraudstream_rag_document_chunks'" in query


def test_deleting_removes_exactly_the_given_chunks_and_commits() -> None:
    connection = FakeConnection()

    online.delete_chunks(connection, "rag", "t", ("b", "c"), 3)

    query, params = connection.executed[0]
    assert "DELETE FROM" in query
    assert params == ([key("b"), key("c")],)
    assert connection.commits == 1


def test_deleting_nothing_runs_no_sql() -> None:
    connection = FakeConnection()

    online.delete_chunks(connection, "rag", "t", (), 3)

    assert connection.executed == [] and connection.commits == 0


def fake_store() -> SimpleNamespace:
    written: list[pd.DataFrame] = []
    return SimpleNamespace(
        written=written,
        config=SimpleNamespace(
            project="fraudstream_rag",
            entity_key_serialization_version=3,
            registry=SimpleNamespace(enable_online_feature_view_versioning=False),
            online_store=SimpleNamespace(
                db_schema="rag", host="pgvector", port=5432, database="llm", user="llm", password="secret", sslmode="disable"
            ),
        ),
        get_feature_view=lambda name: SimpleNamespace(name=name),
        write_to_online_store=lambda view, df: written.append((view, df)),
    )


def chunk_frame(rows: list[tuple[str, str]]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "chunk_id": [chunk_id for chunk_id, _ in rows],
            "doc_id": "handbook/page",
            "source": "handbook",
            "title": "Page",
            "heading": "Signs",
            "text": "text",
            "content_hash": [digest for _, digest in rows],
            "embedding": [pd.array([0.6, 0.8], dtype="float32").to_numpy()] * len(rows),
            "updated_at": pd.Timestamp("2026-10-16", tz="UTC"),
        }
    )


def test_only_new_and_changed_chunks_are_written_and_gone_ones_deleted(monkeypatch: pytest.MonkeyPatch) -> None:
    store = fake_store()
    held = iter([{"a": "h1", "b": "old", "gone": "h9"}, {"a": "h1", "b": "h2", "c": "h3"}])
    deleted: list[tuple] = []
    monkeypatch.setattr(online, "connect", lambda _store: FakeConnection())
    monkeypatch.setattr(online, "stored_hashes", lambda *_: next(held))
    monkeypatch.setattr(online, "delete_chunks", lambda _c, schema, table, ids, version: deleted.append((schema, table, ids, version)))

    summary = online.write_online(store, chunk_frame([("a", "h1"), ("b", "h2"), ("c", "h3")]))

    view, rows = store.written[0]
    assert view == "document_chunks"
    assert list(rows["chunk_id"]) == ["b", "c"]
    assert list(rows.columns) == list(CHUNK_COLUMNS)
    assert rows["embedding"].iloc[0] == [pytest.approx(0.6), pytest.approx(0.8)]
    assert all(type(value) is float for value in rows["embedding"].iloc[0])
    assert deleted == [("rag", "fraudstream_rag_document_chunks", ("gone",), 3)]
    assert summary == {"table": "rag.fraudstream_rag_document_chunks", "rows": 3, "written": 2, "deleted": 1}


def test_nothing_changed_writes_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    store = fake_store()
    monkeypatch.setattr(online, "connect", lambda _store: FakeConnection())
    monkeypatch.setattr(online, "stored_hashes", lambda *_: {"a": "h1"})
    monkeypatch.setattr(online, "delete_chunks", lambda *_: None)

    summary = online.write_online(store, chunk_frame([("a", "h1")]))

    assert store.written == []
    assert (summary["written"], summary["deleted"], summary["rows"]) == (0, 0, 1)


def test_the_connection_uses_the_feast_online_store_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg

    seen: dict = {}
    monkeypatch.setattr(psycopg, "connect", lambda **kwargs: seen.update(kwargs) or "connection")

    assert online.connect(fake_store()) == "connection"
    assert seen == {"host": "pgvector", "port": 5432, "dbname": "llm", "user": "llm", "password": "secret", "sslmode": "disable"}
