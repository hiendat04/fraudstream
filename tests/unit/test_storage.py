"""Unit tests for `fraudstream.storage` -- the MinIO/local object-storage helpers.

The `file://` tests exercise the module against a real temp directory (no
mocking needed). The `s3a://` tests mock `boto3.client` since these are unit
tests, not integration tests against a running MinIO -- they cover the
scheme parsing, the missing-vs-empty-vs-present distinctions, and the
batching/caching behavior added for efficiency.
"""

from __future__ import annotations

import importlib.util
from tempfile import TemporaryDirectory
from unittest import TestCase, main, skipUnless
from unittest.mock import MagicMock, patch

from fraudstream import storage
from fraudstream.jobs.warehouse import WarehouseConfig


class ParseAndJoinUriTest(TestCase):
    """Tests for the small URI-string helpers."""

    def test_parse_s3_uri_splits_bucket_and_key(self) -> None:
        self.assertEqual(storage.parse_s3_uri("s3a://bucket/a/b.csv"), ("bucket", "a/b.csv"))

    def test_parse_s3_uri_rejects_non_s3a_scheme(self) -> None:
        with self.assertRaises(ValueError):
            storage.parse_s3_uri("file:///tmp/x")

    def test_parse_s3_uri_rejects_uri_without_a_key(self) -> None:
        with self.assertRaises(ValueError):
            storage.parse_s3_uri("s3a://bucket")

    def test_join_uri_strips_a_trailing_slash_before_joining(self) -> None:
        self.assertEqual(storage.join_uri("s3a://bucket/raw/", "a", "b.csv"), "s3a://bucket/raw/a/b.csv")


class LocalFileSchemeTest(TestCase):
    """`file://` stands in for MinIO in fast unit tests -- verify it behaves the same."""

    def test_put_get_exists_and_size_round_trip(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            uri = f"file://{tmp_dir}/nested/a.csv"
            warehouse = WarehouseConfig()

            self.assertFalse(storage.object_exists(warehouse, uri))
            self.assertEqual(storage.object_size(warehouse, uri), 0)
            self.assertIsNone(storage.object_size_or_none(warehouse, uri))

            storage.put_text(warehouse, uri, "hello")

            self.assertTrue(storage.object_exists(warehouse, uri))
            self.assertEqual(storage.get_text(warehouse, uri), "hello")
            self.assertEqual(storage.object_size(warehouse, uri), 5)
            self.assertEqual(storage.object_size_or_none(warehouse, uri), 5)

    def test_object_size_or_none_is_zero_not_none_for_an_empty_file(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            uri = f"file://{tmp_dir}/empty.csv"
            warehouse = WarehouseConfig()
            storage.put_text(warehouse, uri, "")

            self.assertEqual(storage.object_size_or_none(warehouse, uri), 0)

    def test_list_keys_filters_by_suffix_and_sorts(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            warehouse = WarehouseConfig()
            storage.put_text(warehouse, f"file://{tmp_dir}/b/transactions.csv", "b")
            storage.put_text(warehouse, f"file://{tmp_dir}/a/transactions.csv", "a")
            storage.put_text(warehouse, f"file://{tmp_dir}/a/_manifest.json", "{}")

            keys = storage.list_keys(warehouse, f"file://{tmp_dir}", suffix="transactions.csv")

            self.assertEqual(
                keys,
                [f"file://{tmp_dir}/a/transactions.csv", f"file://{tmp_dir}/b/transactions.csv"],
            )

    def test_list_keys_returns_empty_for_a_missing_root(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            keys = storage.list_keys(WarehouseConfig(), f"file://{tmp_dir}/does-not-exist")
            self.assertEqual(keys, [])

    def test_delete_prefix_removes_the_whole_tree(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            warehouse = WarehouseConfig()
            uri = f"file://{tmp_dir}/raw/a.csv"
            storage.put_text(warehouse, uri, "data")

            storage.delete_prefix(warehouse, f"file://{tmp_dir}/raw")

            self.assertFalse(storage.object_exists(warehouse, uri))

    def test_delete_prefix_on_a_missing_root_is_a_no_op(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            storage.delete_prefix(WarehouseConfig(), f"file://{tmp_dir}/never-written")


def _not_found_error():
    return _client_error("404", "Not Found")


def _client_error(code: str, message: str):
    import botocore.exceptions

    return botocore.exceptions.ClientError({"Error": {"Code": code, "Message": message}}, "HeadObject")


class _StreamStub:
    """Stand-in for the `StreamingBody` boto3's `get_object` returns."""

    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


@skipUnless(importlib.util.find_spec("boto3"), "boto3 is not installed")
class S3SchemeTest(TestCase):
    """`s3a://` calls go through boto3, mocked here for a fast unit test."""

    def setUp(self) -> None:
        # `_client` is process-cached by WarehouseConfig; clear it so each
        # test's mock is the one actually used, not a previous test's.
        storage._client.cache_clear()
        self.addCleanup(storage._client.cache_clear)

    def _patch_boto3_client(self) -> tuple[MagicMock, MagicMock]:
        patcher = patch("boto3.client")
        mock_client_factory = patcher.start()
        self.addCleanup(patcher.stop)
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client
        return mock_client_factory, mock_client

    def test_client_is_cached_across_calls_for_the_same_warehouse(self) -> None:
        mock_client_factory, mock_client = self._patch_boto3_client()
        mock_client.head_object.side_effect = _not_found_error()
        warehouse = WarehouseConfig()

        storage.object_exists(warehouse, "s3a://bucket/a.csv")
        storage.object_exists(warehouse, "s3a://bucket/b.csv")
        storage.object_exists(warehouse, "s3a://bucket/c.csv")

        mock_client_factory.assert_called_once()

    def test_put_text_writes_utf8_bytes(self) -> None:
        _, mock_client = self._patch_boto3_client()

        storage.put_text(WarehouseConfig(), "s3a://bucket/a.csv", "héllo")

        mock_client.put_object.assert_called_once_with(
            Bucket="bucket", Key="a.csv", Body="héllo".encode("utf-8")
        )

    def test_get_text_decodes_the_response_body(self) -> None:
        _, mock_client = self._patch_boto3_client()
        mock_client.get_object.return_value = {"Body": _StreamStub(b"hello")}

        text = storage.get_text(WarehouseConfig(), "s3a://bucket/a.csv")

        self.assertEqual(text, "hello")

    def test_object_exists_true_when_head_object_succeeds(self) -> None:
        _, mock_client = self._patch_boto3_client()
        mock_client.head_object.return_value = {"ContentLength": 3}

        self.assertTrue(storage.object_exists(WarehouseConfig(), "s3a://bucket/a.csv"))

    def test_object_exists_false_on_404(self) -> None:
        _, mock_client = self._patch_boto3_client()
        mock_client.head_object.side_effect = _not_found_error()

        self.assertFalse(storage.object_exists(WarehouseConfig(), "s3a://bucket/a.csv"))

    def test_object_exists_reraises_non_404_errors(self) -> None:
        _, mock_client = self._patch_boto3_client()
        mock_client.head_object.side_effect = _client_error("500", "InternalError")

        with self.assertRaises(Exception):
            storage.object_exists(WarehouseConfig(), "s3a://bucket/a.csv")

    def test_object_size_is_zero_when_missing(self) -> None:
        _, mock_client = self._patch_boto3_client()
        mock_client.head_object.side_effect = _not_found_error()

        self.assertEqual(storage.object_size(WarehouseConfig(), "s3a://bucket/a.csv"), 0)

    def test_object_size_or_none_distinguishes_missing_from_empty(self) -> None:
        _, mock_client = self._patch_boto3_client()

        mock_client.head_object.side_effect = _not_found_error()
        self.assertIsNone(storage.object_size_or_none(WarehouseConfig(), "s3a://bucket/missing.csv"))

        mock_client.head_object.side_effect = None
        mock_client.head_object.return_value = {"ContentLength": 0}
        self.assertEqual(storage.object_size_or_none(WarehouseConfig(), "s3a://bucket/empty.csv"), 0)

    def test_object_size_or_none_makes_one_head_request(self) -> None:
        """The reason this helper exists: one HEAD request, not the two that
        `object_exists` + `object_size` back to back would make."""

        _, mock_client = self._patch_boto3_client()
        mock_client.head_object.return_value = {"ContentLength": 42}

        storage.object_size_or_none(WarehouseConfig(), "s3a://bucket/a.csv")

        mock_client.head_object.assert_called_once_with(Bucket="bucket", Key="a.csv")

    def test_list_keys_paginates_and_filters_by_suffix(self) -> None:
        _, mock_client = self._patch_boto3_client()
        mock_client.list_objects_v2.side_effect = [
            {
                "Contents": [{"Key": "raw/a/transactions.csv"}, {"Key": "raw/a/_manifest.json"}],
                "IsTruncated": True,
                "NextContinuationToken": "page-2",
            },
            {
                "Contents": [{"Key": "raw/b/transactions.csv"}],
                "IsTruncated": False,
            },
        ]

        keys = storage.list_keys(WarehouseConfig(), "s3a://bucket/raw", suffix="transactions.csv")

        self.assertEqual(
            keys,
            ["s3a://bucket/raw/a/transactions.csv", "s3a://bucket/raw/b/transactions.csv"],
        )
        self.assertEqual(mock_client.list_objects_v2.call_count, 2)
        self.assertEqual(
            mock_client.list_objects_v2.call_args_list[1].kwargs["ContinuationToken"], "page-2"
        )

    def test_delete_prefix_batches_into_one_delete_objects_call(self) -> None:
        _, mock_client = self._patch_boto3_client()
        mock_client.list_objects_v2.return_value = {
            "Contents": [{"Key": f"raw/file-{i}.csv"} for i in range(5)],
            "IsTruncated": False,
        }

        storage.delete_prefix(WarehouseConfig(), "s3a://bucket/raw")

        mock_client.delete_objects.assert_called_once_with(
            Bucket="bucket",
            Delete={"Objects": [{"Key": f"raw/file-{i}.csv"} for i in range(5)]},
        )

    def test_delete_prefix_splits_into_batches_of_1000(self) -> None:
        _, mock_client = self._patch_boto3_client()
        mock_client.list_objects_v2.return_value = {
            "Contents": [{"Key": f"raw/file-{i}.csv"} for i in range(1500)],
            "IsTruncated": False,
        }

        storage.delete_prefix(WarehouseConfig(), "s3a://bucket/raw")

        self.assertEqual(mock_client.delete_objects.call_count, 2)
        first_batch = mock_client.delete_objects.call_args_list[0].kwargs["Delete"]["Objects"]
        second_batch = mock_client.delete_objects.call_args_list[1].kwargs["Delete"]["Objects"]
        self.assertEqual(len(first_batch), 1000)
        self.assertEqual(len(second_batch), 500)


if __name__ == "__main__":
    main()
