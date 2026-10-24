"""Search through Feast and the golden-question check, with a fake store that answers in a known order."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from fraudstream_rag.retrieval import GoldenQuestion, GoldenSet, check_golden, load_golden, search

GOLDEN = Path(__file__).resolve().parents[1] / "checks" / "golden_questions.yaml"


def fake_embed(texts: list[str]) -> np.ndarray:
    return np.full((len(texts), 3), 0.5, dtype=np.float32)


class FakeStore:
    """Answers every search with the same chunks, farthest first, to prove search sorts them.

    Like Feast 0.66, it returns the chunk ids sorted on their own, out of line with the
    other columns; looking the ids up returns each with its own fields.
    """

    def __init__(self, doc_ids: list[str]) -> None:
        self.doc_ids = doc_ids
        self.calls: list[dict] = []
        self.lookups: list[dict] = []

    def retrieve_online_documents_v2(self, **kwargs: object) -> SimpleNamespace:
        self.calls.append(kwargs)
        count = len(self.doc_ids)
        answer = {
            "chunk_id": sorted(f"{doc_id}#s" for doc_id in self.doc_ids),
            "doc_id": self.doc_ids,
            "title": ["T"] * count,
            "heading": ["S"] * count,
            "text": [f"text of {doc_id}" for doc_id in self.doc_ids],
            "distance": [0.5 - 0.1 * index for index in range(count)],
        }
        return SimpleNamespace(to_dict=lambda: answer)

    def get_online_features(self, features: list[str], entity_rows: list[dict]) -> SimpleNamespace:
        self.lookups.append({"features": features, "entity_rows": entity_rows})
        ids = [row["chunk_id"] for row in entity_rows]
        doc_ids = [chunk_id.removesuffix("#s") for chunk_id in ids]
        answer = {
            "chunk_id": ids,
            "doc_id": doc_ids,
            "title": ["T"] * len(ids),
            "heading": ["S"] * len(ids),
            "text": [f"text of {doc_id}" for doc_id in doc_ids],
        }
        return SimpleNamespace(to_dict=lambda: answer)


def test_search_asks_feast_for_the_nearest_chunks_by_cosine_and_returns_nearest_first() -> None:
    store = FakeStore(["handbook/a", "handbook/b"])

    hits = search(store, "many small payments", top_k=2, embed_texts=fake_embed)

    assert [hit["doc_id"] for hit in hits] == ["handbook/b", "handbook/a"]
    assert hits[0] == {"chunk_id": "handbook/b#s", "doc_id": "handbook/b", "title": "T", "heading": "S", "text": "text of handbook/b",
                       "distance": pytest.approx(0.4)}
    call = store.calls[0]
    assert call["features"] == ["document_chunks:doc_id", "document_chunks:title", "document_chunks:heading", "document_chunks:text"]
    assert call["query"] == [0.5, 0.5, 0.5]
    assert (call["top_k"], call["distance_metric"]) == (2, "cosine")


def test_each_hit_keeps_its_own_id_when_feast_returns_the_ids_out_of_line() -> None:
    store = FakeStore(["handbook/c", "handbook/a", "handbook/b"])

    hits = search(store, "q", top_k=3, embed_texts=fake_embed)

    assert [(hit["chunk_id"], hit["text"]) for hit in hits] == [
        ("handbook/b#s", "text of handbook/b"), ("handbook/a#s", "text of handbook/a"), ("handbook/c#s", "text of handbook/c")]
    assert store.lookups == [{"features": store.calls[0]["features"],
                              "entity_rows": [{"chunk_id": "handbook/a#s"}, {"chunk_id": "handbook/b#s"}, {"chunk_id": "handbook/c#s"}]}]


def test_no_hits_needs_no_lookup() -> None:
    store = FakeStore([])

    assert search(store, "q", top_k=3, embed_texts=fake_embed) == []
    assert store.lookups == []


def test_a_hit_whose_chunk_changed_between_the_reads_is_left_out() -> None:
    store = FakeStore(["handbook/a", "handbook/b"])
    looked_up = store.get_online_features

    def rewritten(features: list[str], entity_rows: list[dict]) -> SimpleNamespace:
        answer = looked_up(features, entity_rows).to_dict()
        answer["text"][0] = "new text"
        return SimpleNamespace(to_dict=lambda: answer)

    store.get_online_features = rewritten

    assert [hit["chunk_id"] for hit in search(store, "q", top_k=2, embed_texts=fake_embed)] == ["handbook/b#s"]


def golden(min_hit_rate: float) -> GoldenSet:
    return GoldenSet(
        questions=(GoldenQuestion("q1", "model-cards/"), GoldenQuestion("q2", "handbook/missing")),
        top_k=5,
        min_hit_rate=min_hit_rate,
    )


def test_a_question_hits_when_any_top_chunk_is_from_its_document() -> None:
    result = check_golden(FakeStore(["handbook/a", "model-cards/fraud-detection-v4"]), golden(0.5), fake_embed)

    assert (result["questions"], result["hits"], result["hit_rate"]) == (2, 1, 0.5)
    assert result["misses"] == [{"question": "q2", "expect": "handbook/missing", "found": ["model-cards/fraud-detection-v4#s", "handbook/a#s"]}]


@pytest.mark.parametrize(("min_hit_rate", "passed"), [(0.5, True), (0.51, False)])
def test_the_check_passes_at_the_bar_and_fails_just_above_it(min_hit_rate: float, passed: bool) -> None:
    result = check_golden(FakeStore(["model-cards/fraud-detection-v4"]), golden(min_hit_rate), fake_embed)

    assert result["passed"] is passed


def test_the_golden_set_is_twelve_questions_at_top_five() -> None:
    loaded = load_golden(GOLDEN)

    assert (len(loaded.questions), loaded.top_k, loaded.min_hit_rate) == (12, 5, 0.9)
    assert loaded.questions[0] == GoldenQuestion("Many small payments on one card within a few minutes. What is going on?", "handbook/typology-card-testing")


def test_a_golden_file_without_questions_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "golden.yaml"
    path.write_text("top_k: 5\nmin_hit_rate: 0.9\nquestions: []\n")

    with pytest.raises(ValueError, match="no golden questions"):
        load_golden(path)


def test_the_hit_rate_is_rounded_to_four_places() -> None:
    three = GoldenSet(
        questions=(GoldenQuestion("q1", "handbook/a"), GoldenQuestion("q2", "handbook/x"), GoldenQuestion("q3", "handbook/y")),
        top_k=5,
        min_hit_rate=0.3,
    )

    assert check_golden(FakeStore(["handbook/a"]), three, fake_embed)["hit_rate"] == 0.3333
