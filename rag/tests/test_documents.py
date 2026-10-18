"""The documents: model cards, feature view pages, the handbook, and what makes a set safe to index."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from feast import Entity, FeatureView, Field, FileSource, ValueType
from feast.types import Float64, Int64, String

from fraudstream_rag.chunking import Document, chunk_document
from fraudstream_rag.documents import (
    MAX_CHARACTERS,
    NOT_RECORDED,
    feature_view_page,
    handbook_page,
    model_card,
    validate,
)

HANDBOOK = Path(__file__).resolve().parents[1] / "handbook"
RUN = {
    "run_id": "a861c334",
    "metrics": {"threshold": 0.6264204382896423, "pr_auc": 0.0283, "roc_auc": 0.6584, "precision": 0.0291, "recall": 0.2432},
    "params": {"data_snapshot_id": "1411361247587751803", "data_table": "iceberg.ml.training_data", "max_depth": "3", "learning_rate": "0.1", "num_boost_round": "400", "num_nodes": "2"},
}
CARD = Document("model-cards/fraud-detection-v4", "model_card", "# Model card\n\nVersion 4.\n")
PAGE = Document("feature-dictionary/merchant_risk_features", "feature_dictionary", "# Feature view\n\nMerchants.\n")
POLICY = Document("handbook/review-policy", "handbook", "# Policy\n\nThe model decides.\n")


def card(aliases: list[str], newest: bool = False, run: dict = RUN) -> str:
    return model_card("fraud-detection", "4", aliases=aliases, newest=newest, created_ms=1791998599413, run=run).text


def test_a_card_states_the_threshold_quality_data_and_settings() -> None:
    assert card(["production"], newest=True) == (
        "# Model card: fraud-detection version 4 (production)\n\n"
        "Version 4 of the fraud detection model `fraud-detection`. It is the production model: it scores every payment.\n\n"
        "## Decision threshold\n\n"
        "A payment is held for review when the score of version 4 is at or above 0.6264.\n\n"
        "## Quality on validation data\n\n"
        "PR-AUC 0.028, ROC-AUC 0.658, precision 0.029 and recall 0.243 at the threshold. "
        "PR-AUC is the main measure, because fraud is rare.\n\n"
        "## Training data\n\n"
        "Trained on snapshot 1411361247587751803 of `iceberg.ml.training_data`. "
        "Registered on 2026-10-14 from MLflow run a861c334.\n\n"
        "## Settings\n\n"
        "XGBoost with max_depth 3, learning_rate 0.1 and num_boost_round 400, trained on 2 nodes.\n"
    )


@pytest.mark.parametrize(
    ("aliases", "newest", "status", "sentence"),
    [
        (["production"], True, "production", "it scores every payment"),
        ([], True, "newest", "only while a person runs it as a canary"),
        ([], False, "older", "serves no traffic"),
    ],
)
def test_a_card_says_whether_the_version_serves_traffic(aliases, newest, status, sentence) -> None:
    text = card(aliases, newest)

    assert f"({status})" in text.splitlines()[0]
    assert sentence in text


def test_a_run_that_recorded_nothing_still_gets_a_card() -> None:
    text = card([], run={})

    assert f"at or above {NOT_RECORDED}." in text
    assert f"MLflow run {NOT_RECORDED}" in text


def merchant_view(ttl: timedelta | None, tags: dict[str, str] | None = None) -> FeatureView:
    return FeatureView(
        name="merchant_risk_features",
        entities=[Entity(name="merchant", join_keys=["merchant_id"], value_type=ValueType.STRING)],
        ttl=ttl,
        schema=[
            Field(name="merchant_category", dtype=String),
            Field(name="merchant_txn_count_1d", dtype=Int64),
            Field(name="merchant_prior_fraud_rate_30d", dtype=Float64),
        ],
        source=FileSource(name="merchant_risk_source", path="risk.parquet", timestamp_field="event_timestamp", description="Merchant risk snapshots."),
        tags=tags or {},
    )


def test_a_feature_view_page_lists_its_entity_features_and_source() -> None:
    page = feature_view_page(merchant_view(timedelta(days=31), {"layer": "gold"}), {"merchant": "A generated merchant."})

    assert (page.doc_id, page.source) == ("feature-dictionary/merchant_risk_features", "feature_dictionary")
    assert page.text == (
        "# Feature view: merchant_risk_features\n\n"
        "Features per merchant (A generated merchant.). Values older than 31 days are not served. Tags: layer gold.\n\n"
        "## Features\n\n"
        "- `merchant_category` (text)\n"
        "- `merchant_txn_count_1d` (integer)\n"
        "- `merchant_prior_fraud_rate_30d` (number)\n\n"
        "## Source\n\n"
        "merchant_risk_source: Merchant risk snapshots.\n"
    )


@pytest.mark.parametrize(
    ("ttl", "expected"),
    [(timedelta(hours=1), "Values older than 1 hours"), (timedelta(days=1), "Values older than 1 days"), (None, "Features per merchant (no description). Tags: none.")],
)
def test_a_feature_view_page_states_how_fresh_its_values_are(ttl, expected) -> None:
    assert expected in feature_view_page(merchant_view(ttl), {}).text


def test_a_handbook_page_is_named_after_its_file() -> None:
    assert handbook_page("card-testing.md", "# Card testing\n") == Document("handbook/card-testing", "handbook", "# Card testing\n")


def test_one_document_from_each_source_is_safe_to_index() -> None:
    assert validate([POLICY, CARD, PAGE]) == []


@pytest.mark.parametrize("missing", ["handbook", "model_card", "feature_dictionary"])
def test_a_missing_source_stops_the_run(missing: str) -> None:
    problems = validate([document for document in (POLICY, CARD, PAGE) if document.source != missing])

    assert problems == [f"no documents from {missing}: indexing would delete every {missing} chunk"]


@pytest.mark.parametrize(
    ("document", "problem"),
    [
        (Document("Handbook/Policy", "handbook", "# T\n\ntext\n"), "id must look like source/name"),
        (Document("handbook/untitled", "handbook", "## Only a section\n\ntext\n"), "no '# ' title"),
        (Document("handbook/empty", "handbook", "# Title\n\n## Heading\n"), "no text under the headings"),
        (Document("handbook/long", "handbook", "# T\n\n" + "x" * MAX_CHARACTERS), f"longer than {MAX_CHARACTERS}"),
        (Document("handbook/card", "handbook", "# T\n\nThe test card 4111 1111 1111 1111 was used.\n"), "looks like a card number"),
        (Document("notes/x", "notes", "# T\n\ntext\n"), "unknown source notes"),
    ],
)
def test_an_unsafe_document_is_reported(document: Document, problem: str) -> None:
    problems = validate([POLICY, CARD, PAGE, document])

    assert len(problems) == 1
    assert problem in problems[0]


def test_two_documents_with_one_id_are_reported() -> None:
    assert validate([POLICY, POLICY, CARD, PAGE]) == ["handbook/review-policy: two documents have this id"]


def test_exactly_max_characters_is_allowed() -> None:
    text = "# T\n\n" + "x" * (MAX_CHARACTERS - len("# T\n\n"))

    assert validate([Document("handbook/long", "handbook", text), CARD, PAGE]) == []


def test_digits_that_fail_the_card_check_are_not_card_numbers() -> None:
    document = Document("handbook/ids", "handbook", "# T\n\nCase 4111 1111 1111 1112 is closed.\n")

    assert validate([document, CARD, PAGE]) == []


def test_only_the_handbook_is_checked_for_card_numbers() -> None:
    snapshot = Document("model-cards/fraud-detection-v9", "model_card", "# Card\n\nSnapshot 4111111111111111.\n")

    assert validate([POLICY, snapshot, PAGE]) == []


def test_the_handbook_is_safe_to_index_and_every_page_has_sections() -> None:
    pages = [handbook_page(path.name, path.read_text(encoding="utf-8")) for path in sorted(HANDBOOK.glob("*.md"))]

    assert len(pages) == 7
    assert validate([*pages, CARD, PAGE]) == []
    assert all(len(chunk_document(page, "m")) >= 3 for page in pages)
