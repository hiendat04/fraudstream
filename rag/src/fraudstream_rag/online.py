"""The online copy: Feast's pgvector store, which the context API searches.

Feast writes the chunks and searches them. Feast 0.66 cannot list or delete what an
online store holds, so those two use SQL on the table Feast made for the feature
view: one row per chunk and feature, keyed by Feast's serialized entity key.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from fraudstream_rag.changes import plan_changes
from fraudstream_rag.offline import plain_rows

FEATURE_VIEW = "document_chunks"
JOIN_KEY = "chunk_id"


def write_online(store: Any, frame: pd.DataFrame) -> dict[str, Any]:
    """Write the chunks whose hash changed and delete the ones that are gone; say what changed."""

    schema, table = table_name(store)
    version = store.config.entity_key_serialization_version
    with connect(store) as connection:
        stored = stored_hashes(connection, schema, table, version)
        changes = plan_changes(stored, dict(zip(frame["chunk_id"], frame["content_hash"])))
        rows = plain_rows(frame[frame["chunk_id"].isin(changes.write)])
        if len(rows):
            store.write_to_online_store(FEATURE_VIEW, df=rows)
        delete_chunks(connection, schema, table, changes.delete, version)
        held = len(stored_hashes(connection, schema, table, version))
    return {
        "table": f"{schema}.{table}",
        "rows": held,
        "written": len(changes.write),
        "deleted": len(changes.delete),
    }


def stored_hashes(connection: Any, schema: str, table: str, version: int) -> dict[str, str]:
    from feast.infra.key_encoding_utils import deserialize_entity_key
    from psycopg import sql

    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT entity_key, value_text FROM {} WHERE feature_name = 'content_hash'").format(
                sql.Identifier(schema, table)
            )
        )
        rows = cursor.fetchall()
    return {
        deserialize_entity_key(bytes(key), entity_key_serialization_version=version)
        .entity_values[0]
        .string_val: digest
        for key, digest in rows
    }


def delete_chunks(connection: Any, schema: str, table: str, chunk_ids: tuple[str, ...], version: int) -> None:
    if not chunk_ids:
        return
    from feast.infra.key_encoding_utils import serialize_entity_key
    from feast.protos.feast.types.EntityKey_pb2 import EntityKey
    from feast.protos.feast.types.Value_pb2 import Value
    from psycopg import sql

    keys = [
        serialize_entity_key(
            EntityKey(join_keys=[JOIN_KEY], entity_values=[Value(string_val=chunk_id)]),
            entity_key_serialization_version=version,
        )
        for chunk_id in chunk_ids
    ]
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("DELETE FROM {} WHERE entity_key = ANY(%s)").format(sql.Identifier(schema, table)),
            (keys,),
        )
    connection.commit()


def table_name(store: Any) -> tuple[str, str]:
    from feast.infra.online_stores.helpers import compute_table_id

    return (
        store.config.online_store.db_schema,
        compute_table_id(
            store.config.project,
            store.get_feature_view(FEATURE_VIEW),
            store.config.registry.enable_online_feature_view_versioning,
        ),
    )


def connect(store: Any) -> Any:
    import psycopg

    online = store.config.online_store
    return psycopg.connect(
        host=online.host,
        port=online.port,
        dbname=online.database,
        user=online.user,
        password=online.password,
        sslmode=online.sslmode,
    )
