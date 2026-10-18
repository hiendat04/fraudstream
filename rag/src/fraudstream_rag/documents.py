"""The copilot's documents: the handbook, a card per model version, a page per feature view.

Everything here is pure: the inputs are what MLflow, Feast and MinIO returned, and
the output is Markdown. `validate` decides whether a set of documents is safe to
index. An empty source would make the index delete every chunk that came from it,
so a missing source stops the run instead.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any, Mapping

from fraudstream_rag.chunking import Document, title_of

SOURCES = ("handbook", "model_card", "feature_dictionary")
MAX_CHARACTERS = 100_000
TYPE_NAMES = {
    "Int32": "integer",
    "Int64": "integer",
    "Float32": "number",
    "Float64": "number",
    "String": "text",
    "Bool": "true or false",
}
_DOC_ID = re.compile(r"^[a-z-]+/[a-z0-9][a-z0-9._-]*$")
_CARD_NUMBER = re.compile(r"(?<!\d)(?:\d[ -]?){12,18}\d(?!\d)")
NOT_RECORDED = "not recorded"


def handbook_page(file_name: str, text: str) -> Document:
    return Document(doc_id=f"handbook/{file_name.removesuffix('.md')}", source="handbook", text=text)


def model_card(
    model: str,
    version: str,
    *,
    aliases: list[str],
    newest: bool,
    created_ms: int,
    run: Mapping[str, Any],
) -> Document:
    """One card per registered version, from what its training run recorded."""

    metrics = run.get("metrics", {})
    params = run.get("params", {})
    if "production" in aliases:
        status, state = "production", "It is the production model: it scores every payment."
    elif newest:
        status, state = "newest", "It is the newest version. It serves traffic only while a person runs it as a canary."
    else:
        status, state = "older", "It is an older version and serves no traffic."
    created = datetime.fromtimestamp(created_ms / 1000, tz=UTC).date().isoformat()

    def metric(name: str, digits: int = 3) -> str:
        return f"{float(metrics[name]):.{digits}f}" if name in metrics else NOT_RECORDED

    def param(name: str) -> str:
        return str(params.get(name, NOT_RECORDED))

    text = f"""# Model card: {model} version {version} ({status})

Version {version} of the fraud detection model `{model}`. {state}

## Decision threshold

A payment is held for review when the score of version {version} is at or above {metric("threshold", 4)}.

## Quality on validation data

PR-AUC {metric("pr_auc")}, ROC-AUC {metric("roc_auc")}, precision {metric("precision")} and recall {metric("recall")} at the threshold. PR-AUC is the main measure, because fraud is rare.

## Training data

Trained on snapshot {param("data_snapshot_id")} of `{param("data_table")}`. Registered on {created} from MLflow run {run.get("run_id", NOT_RECORDED)}.

## Settings

XGBoost with max_depth {param("max_depth")}, learning_rate {param("learning_rate")} and num_boost_round {param("num_boost_round")}, trained on {param("num_nodes")} nodes.
"""
    return Document(doc_id=f"model-cards/{model}-v{version}", source="model_card", text=text)


def feature_view_page(view: Any, entity_descriptions: Mapping[str, str]) -> Document:
    """One page per Feast feature view: what it describes, what it holds, where it comes from."""

    entities = "; ".join(
        f"{name} ({entity_descriptions.get(name, 'no description')})" for name in view.entities
    )
    features = "\n".join(
        f"- `{field.name}` ({TYPE_NAMES.get(str(field.dtype), str(field.dtype).lower())})"
        for field in view.features
    )
    seconds = view.ttl.total_seconds() if view.ttl else 0
    if seconds >= 86400:
        freshness = f" Values older than {seconds / 86400:g} days are not served."
    elif seconds:
        freshness = f" Values older than {seconds / 3600:g} hours are not served."
    else:
        freshness = ""
    tags = ", ".join(f"{key} {value}" for key, value in sorted(view.tags.items()))
    source = view.stream_source or view.batch_source
    text = f"""# Feature view: {view.name}

Features per {entities}.{freshness} Tags: {tags or "none"}.

## Features

{features}

## Source

{source.name}: {source.description or "no description"}
"""
    return Document(doc_id=f"feature-dictionary/{view.name}", source="feature_dictionary", text=text)


def validate(documents: list[Document]) -> list[str]:
    """Every reason not to index these documents; an empty list means they are safe."""

    problems = [
        f"no documents from {source}: indexing would delete every {source} chunk"
        for source in SOURCES
        if not any(document.source == source for document in documents)
    ]
    seen: set[str] = set()
    for document in documents:
        if document.source not in SOURCES:
            problems.append(f"{document.doc_id}: unknown source {document.source}")
        if not _DOC_ID.match(document.doc_id):
            problems.append(f"{document.doc_id}: id must look like source/name in lower case")
        if document.doc_id in seen:
            problems.append(f"{document.doc_id}: two documents have this id")
        seen.add(document.doc_id)
        if not title_of(document.text):
            problems.append(f"{document.doc_id}: no '# ' title on any line")
        if not any(line.strip() and not line.lstrip().startswith("#") for line in document.text.splitlines()):
            problems.append(f"{document.doc_id}: no text under the headings")
        if len(document.text) > MAX_CHARACTERS:
            problems.append(f"{document.doc_id}: longer than {MAX_CHARACTERS} characters")
        if document.source == "handbook" and any(
            _luhn(re.sub(r"\D", "", match)) for match in _CARD_NUMBER.findall(document.text)
        ):
            problems.append(f"{document.doc_id}: contains what looks like a card number")
    return problems


def _luhn(digits: str) -> bool:
    total = 0
    for position, digit in enumerate(reversed(digits)):
        value = int(digit) * (2 if position % 2 else 1)
        total += value - 9 if value > 9 else value
    return total % 10 == 0
