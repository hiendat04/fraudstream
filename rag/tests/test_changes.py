"""The change plan: write what is new or different, delete what is gone, never empty a store by accident."""

from __future__ import annotations

import pytest

from fraudstream_rag.changes import Changes, plan_changes


def test_a_new_chunk_is_written() -> None:
    assert plan_changes({}, {"a": "h1"}) == Changes(write=("a",), delete=(), unchanged=0)


def test_a_chunk_whose_hash_changed_is_written_again() -> None:
    assert plan_changes({"a": "h1"}, {"a": "h2"}).write == ("a",)


def test_an_unchanged_chunk_is_left_alone() -> None:
    changes = plan_changes({"a": "h1", "b": "h2"}, {"a": "h1", "b": "h2"})

    assert changes == Changes(write=(), delete=(), unchanged=2)
    assert changes.empty


def test_a_chunk_that_is_gone_is_deleted() -> None:
    assert plan_changes({"a": "h1", "b": "h2"}, {"a": "h1"}) == Changes(write=(), delete=("b",), unchanged=1)


def test_ids_come_back_sorted() -> None:
    assert plan_changes({"z": "1", "y": "1"}, {"c": "1", "a": "1"}) == Changes(write=("a", "c"), delete=("y", "z"), unchanged=0)


def test_planning_to_keep_nothing_is_refused() -> None:
    with pytest.raises(ValueError, match="refusing to delete every stored chunk"):
        plan_changes({"a": "h1"}, {})


def test_an_empty_store_and_no_chunks_is_nothing_to_do() -> None:
    assert plan_changes({}, {}).empty
