"""The copilot's document chunks as a Feast feature view, searched by vector in pgvector."""

from __future__ import annotations

from datetime import timedelta

from feast import Entity, FeatureView, Field, ValueType
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
from feast.types import Array, Float32, String

from fraudstream_rag.embedding import DIMENSIONS

chunk = Entity(
    name="chunk",
    join_keys=["chunk_id"],
    value_type=ValueType.STRING,
    description="One section of a handbook page, a model card or a feature view page.",
)

chunks_source = SparkSource(
    name="chunks_source",
    table="iceberg.rag.chunks",
    timestamp_field="updated_at",
    description="Every chunk the RAG pipeline indexed, with its embedding; one Iceberg snapshot per change.",
)

document_chunks = FeatureView(
    name="document_chunks",
    entities=[chunk],
    ttl=timedelta(0),
    schema=[
        Field(name="chunk_id", dtype=String),
        Field(name="doc_id", dtype=String),
        Field(name="source", dtype=String),
        Field(name="title", dtype=String),
        Field(name="heading", dtype=String),
        Field(name="text", dtype=String),
        Field(name="content_hash", dtype=String),
        Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=DIMENSIONS,
            vector_search_metric="cosine",
        ),
    ],
    source=chunks_source,
    online=True,
    tags={"layer": "rag"},
)
