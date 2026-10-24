"""Search the online store the way the context API will, and check it with golden questions."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import numpy as np
import yaml

from fraudstream_rag.embedding import embed
from fraudstream_rag.online import FEATURE_VIEW, JOIN_KEY

RETURNED = ("doc_id", "title", "heading", "text")


@dataclass(frozen=True)
class GoldenQuestion:
    question: str
    expect: str


@dataclass(frozen=True)
class GoldenSet:
    questions: tuple[GoldenQuestion, ...]
    top_k: int
    min_hit_rate: float


def load_golden(path: Path) -> GoldenSet:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    questions = tuple(GoldenQuestion(item["question"], item["expect"]) for item in raw["questions"])
    if not questions:
        raise ValueError(f"{path} has no golden questions")
    return GoldenSet(questions=questions, top_k=int(raw["top_k"]), min_hit_rate=float(raw["min_hit_rate"]))


def search(
    store: Any,
    question: str,
    top_k: int = 5,
    embed_texts: Callable[[list[str]], np.ndarray] = embed,
) -> list[dict[str, Any]]:
    """The top_k nearest chunks, nearest first, each with its cosine distance."""

    features = [f"{FEATURE_VIEW}:{name}" for name in RETURNED]
    response = store.retrieve_online_documents_v2(
        features=features,
        query=embed_texts([question])[0].tolist(),
        top_k=top_k,
        distance_metric="cosine",
    ).to_dict()
    if not response[JOIN_KEY]:
        return []
    # Feast 0.66 sorts the chunk ids apart from the other columns, so each hit is paired
    # with its id again by looking the ids up, the way a single chunk is read.
    stored = store.get_online_features(
        features=features, entity_rows=[{JOIN_KEY: chunk_id} for chunk_id in response[JOIN_KEY]]
    ).to_dict()
    id_of = {tuple(stored[name][index] for name in RETURNED): chunk_id for index, chunk_id in enumerate(stored[JOIN_KEY])}
    hits = []
    for index in range(len(response[JOIN_KEY])):
        fields = tuple(response[name][index] for name in RETURNED)
        # A chunk rewritten between the two reads has no id to cite, so it is left out.
        if fields in id_of:
            hits.append({"chunk_id": id_of[fields], **dict(zip(RETURNED, fields)), "distance": float(response["distance"][index])})
    return sorted(hits, key=lambda hit: hit["distance"])


def check_golden(
    store: Any,
    golden: GoldenSet,
    embed_texts: Callable[[list[str]], np.ndarray] = embed,
) -> dict[str, Any]:
    """A golden question hits when a chunk of its expected document is in the top_k."""

    misses = []
    for item in golden.questions:
        found = search(store, item.question, golden.top_k, embed_texts)
        if not any(hit["doc_id"].startswith(item.expect) for hit in found):
            misses.append({"question": item.question, "expect": item.expect, "found": [hit["chunk_id"] for hit in found]})
    total = len(golden.questions)
    hit_rate = (total - len(misses)) / total
    return {
        "questions": total,
        "hits": total - len(misses),
        "hit_rate": round(hit_rate, 4),
        "top_k": golden.top_k,
        "min_hit_rate": golden.min_hit_rate,
        "misses": misses,
        "passed": hit_rate >= golden.min_hit_rate,
    }
