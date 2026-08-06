"""Thin object-storage helpers for raw data that no Spark session touches.

`fraudstream.jobs.warehouse` configures *Spark's* view of MinIO (`s3a://`
URIs consumed by Hadoop's S3A filesystem, wired into a `SparkSession`). This
module is for code that has no Spark session at all -- the offline generator
writing raw CSV partitions, and Bronze's raw-source discovery peeking at the
manifest and sniffing one file's header before Spark reads it. Both reuse
`fraudstream.jobs.warehouse.WarehouseConfig` for endpoint/credentials, so
there is one source of truth for MinIO connection settings.

Every function here accepts a URI with one of two schemes:
- `s3a://bucket/key` -- real MinIO, read/written via `boto3`.
- `file:///abs/path` -- a local path, read/written via `pathlib`. Unit tests
  use this so they can exercise the same code paths without a running MinIO,
  the same way the Iceberg catalog tests use a local `file://` warehouse
  instead of real MinIO/Postgres (see `fraudstream.jobs.warehouse.IcebergCatalogConfig`).
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

from fraudstream.jobs.warehouse import WarehouseConfig

_S3_SCHEME = "s3a://"
_FILE_SCHEME = "file://"


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split an `s3a://bucket/key/...` URI into `(bucket, key)`."""

    if not uri.startswith(_S3_SCHEME):
        raise ValueError(f"Expected an {_S3_SCHEME} URI, got: {uri!r}")
    without_scheme = uri[len(_S3_SCHEME):]
    bucket, _, key = without_scheme.partition("/")
    if not bucket or not key:
        raise ValueError(f"Expected {_S3_SCHEME}bucket/key, got: {uri!r}")
    return bucket, key


def join_uri(uri: str, *parts: str) -> str:
    """Join a root URI (either scheme) with one or more path segments."""

    return "/".join((uri.rstrip("/"), *parts))


def put_text(warehouse: WarehouseConfig, uri: str, text: str) -> None:
    """Write UTF-8 text to one object, creating/overwriting it."""

    if uri.startswith(_FILE_SCHEME):
        path = _local_path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return

    bucket, key = parse_s3_uri(uri)
    _client(warehouse).put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"))


def get_text(warehouse: WarehouseConfig, uri: str) -> str:
    """Read one object as UTF-8 text."""

    if uri.startswith(_FILE_SCHEME):
        return _local_path(uri).read_text(encoding="utf-8")

    bucket, key = parse_s3_uri(uri)
    response = _client(warehouse).get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8")


def object_exists(warehouse: WarehouseConfig, uri: str) -> bool:
    """Return whether one object exists."""

    if uri.startswith(_FILE_SCHEME):
        return _local_path(uri).exists()

    bucket, key = parse_s3_uri(uri)
    return _head_object(warehouse, bucket, key) is not None


def object_size(warehouse: WarehouseConfig, uri: str) -> int:
    """Return one object's size in bytes, or 0 if it doesn't exist."""

    if uri.startswith(_FILE_SCHEME):
        path = _local_path(uri)
        return path.stat().st_size if path.exists() else 0

    bucket, key = parse_s3_uri(uri)
    response = _head_object(warehouse, bucket, key)
    return int(response["ContentLength"]) if response is not None else 0


def object_size_or_none(warehouse: WarehouseConfig, uri: str) -> int | None:
    """Return one object's size in bytes, or None if it doesn't exist.

    Unlike `object_size`, this distinguishes "missing" from "exists but
    empty" (both would otherwise read as `0`) in a single round trip --
    for a caller that needs both facts, this halves the HEAD requests
    against `object_exists` + `object_size` back to back.
    """

    if uri.startswith(_FILE_SCHEME):
        path = _local_path(uri)
        return path.stat().st_size if path.exists() else None

    bucket, key = parse_s3_uri(uri)
    response = _head_object(warehouse, bucket, key)
    return int(response["ContentLength"]) if response is not None else None


def list_keys(warehouse: WarehouseConfig, uri_prefix: str, suffix: str | None = None) -> list[str]:
    """List object URIs under a prefix, optionally filtered by suffix.

    Returns full URIs (same scheme as `uri_prefix`), sorted, so callers can
    treat the result the way they'd treat a local `Path.glob()` result.
    """

    if uri_prefix.startswith(_FILE_SCHEME):
        root = _local_path(uri_prefix)
        if not root.exists():
            return []
        matches = [
            path for path in root.rglob("*")
            if path.is_file() and (suffix is None or path.name.endswith(suffix))
        ]
        return sorted(f"{_FILE_SCHEME}{path}" for path in matches)

    bucket, prefix = parse_s3_uri(uri_prefix)
    client = _client(warehouse)
    keys: list[str] = []
    continuation_token: str | None = None
    while True:
        list_kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            list_kwargs["ContinuationToken"] = continuation_token
        response = client.list_objects_v2(**list_kwargs)
        for obj in response.get("Contents", []):
            if suffix is None or obj["Key"].endswith(suffix):
                keys.append(f"{_S3_SCHEME}{bucket}/{obj['Key']}")
        if not response.get("IsTruncated"):
            break
        continuation_token = response.get("NextContinuationToken")
    return sorted(keys)


def delete_prefix(warehouse: WarehouseConfig, uri_prefix: str) -> None:
    """Delete every object under a prefix, so a regenerated run starts clean.

    Used before writing a fresh set of partitions, so a run with a shorter
    `days_history` (or otherwise different partition set) than a prior run
    doesn't leave stale files mixed in -- the same intent the old local
    `shutil.rmtree` on `schema_version=*` directories had.
    """

    if uri_prefix.startswith(_FILE_SCHEME):
        import shutil

        root = _local_path(uri_prefix)
        if root.exists():
            shutil.rmtree(root)
        return

    bucket, _ = parse_s3_uri(uri_prefix)
    client = _client(warehouse)
    keys = [parse_s3_uri(uri)[1] for uri in list_keys(warehouse, uri_prefix)]
    # S3's batch-delete API takes up to 1000 keys per call, so a generator
    # run's worth of partitions (a few hundred files) is one round trip
    # instead of one DELETE per file.
    for batch_start in range(0, len(keys), 1000):
        batch = keys[batch_start : batch_start + 1000]
        client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": key} for key in batch]})


def _local_path(uri: str) -> Path:
    """Resolve a `file://` URI to a local `Path`."""

    return Path(uri[len(_FILE_SCHEME):])


def _head_object(warehouse: WarehouseConfig, bucket: str, key: str) -> dict[str, Any] | None:
    """Return one object's HEAD response, or None if it doesn't exist."""

    import botocore.exceptions

    try:
        return _client(warehouse).head_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in {"404", "NoSuchKey"}:
            return None
        raise


@lru_cache(maxsize=8)
def _client(warehouse: WarehouseConfig) -> Any:
    """Build (and cache) a boto3 S3 client pointed at MinIO.

    `WarehouseConfig` is a frozen, hashable dataclass, so this is keyed on
    its actual endpoint/credentials -- every `storage` call for the same
    warehouse reuses one client instead of paying boto3's session/credential
    setup cost per call, which matters here since callers like the offline
    generator and Bronze's source discovery call into this module once per
    file, potentially hundreds of times in a single run.
    """

    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is not installed. Run `uv sync --extra storage`, then retry this command."
        ) from exc

    return boto3.client(
        "s3",
        endpoint_url=warehouse.endpoint,
        aws_access_key_id=warehouse.access_key,
        aws_secret_access_key=warehouse.secret_key,
    )
