"""The steps as Airflow runs them: each reads the step before it from the run folder."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from fraudstream_rag import steps
from fraudstream_rag.chunking import Document
from fraudstream_rag.embedding import DIMENSIONS, MODEL_ID

DOCUMENTS = [
    Document("handbook/card-testing", "handbook", "# Card testing\n\n## Signs\n\nMany small payments.\n\n## What to do\n\nHold the card.\n"),
    Document("model-cards/fraud-detection-v4", "model_card", "# Model card\n\n## Decision threshold\n\n0.62\n"),
    Document("feature-dictionary/merchant_risk_features", "feature_dictionary", "# Feature view\n\n## Features\n\n- merchant_txn_count_1d\n"),
]


def unit_vectors(texts: list[str]) -> np.ndarray:
    vectors = np.zeros((len(texts), DIMENSIONS), dtype=np.float32)
    vectors[:, 0] = 1.0
    return vectors


def prepared(run_dir: Path, documents: list[Document] = DOCUMENTS) -> Path:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "documents.json").write_text(json.dumps([asdict(document) for document in documents]))
    return run_dir


def read(path: Path) -> object:
    return json.loads(path.read_text())


def test_validate_chunk_and_embed_leave_their_files_in_the_run_folder(tmp_path: Path) -> None:
    run_dir = prepared(tmp_path / "run")

    assert steps.validate_documents(run_dir) == []
    assert steps.chunk(run_dir) == 4
    summary = steps.embed_chunks(run_dir, unit_vectors, now=datetime(2026, 10, 16, tzinfo=UTC))

    assert read(run_dir / "validation.json") == {
        "documents": {"handbook": 1, "model_card": 1, "feature_dictionary": 1},
        "problems": [],
    }
    assert summary == {"model": MODEL_ID, "chunks": 4, "dimensions": [DIMENSIONS], "min_norm": 1.0, "max_norm": 1.0}
    frame = pd.read_parquet(run_dir / "chunks.parquet")
    assert list(frame["chunk_id"])[:2] == ["handbook/card-testing#signs", "handbook/card-testing#what-to-do"]
    assert len(frame["embedding"].iloc[0]) == DIMENSIONS
    assert frame["updated_at"].iloc[0] == pd.Timestamp("2026-10-16", tz="UTC")


def test_an_unsafe_set_exits_1_from_validate(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    run_dir = prepared(tmp_path / "run", DOCUMENTS[:2])

    with pytest.raises(SystemExit) as exit_:
        steps.main(["validate", "--run-dir", str(run_dir)])

    assert exit_.value.code == 1
    assert "no documents from feature_dictionary" in capsys.readouterr().out


def test_load_writes_every_source_into_documents_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import fraudstream_rag.sources as sources

    monkeypatch.setattr(sources, "handbook_documents", lambda warehouse, uri: [DOCUMENTS[0]])
    monkeypatch.setattr(sources, "model_card_documents", lambda client: [DOCUMENTS[1]])
    monkeypatch.setattr(sources, "feature_dictionary_documents", lambda store: [DOCUMENTS[2]])

    counts = steps.load(tmp_path, warehouse=None, handbook_uri="s3a://fraudstream/rag/handbook", mlflow=None, feature_store=None)

    assert counts == {"handbook": 1, "model_card": 1, "feature_dictionary": 1}
    assert [item["doc_id"] for item in read(tmp_path / "documents.json")] == [document.doc_id for document in DOCUMENTS]


def test_summarize_reports_a_clean_run_as_passed(tmp_path: Path) -> None:
    run_dir = tmp_path / "scheduled__2026-10-16T040000"
    run_dir.mkdir()
    parts = {
        "validation": {"documents": {"handbook": 7}, "problems": []},
        "embed": {"chunks": 71},
        "offline": {"rows": 71},
        "online": {"rows": 71},
        "retrieval": {"passed": True},
    }
    for name, payload in parts.items():
        (run_dir / f"{name}.json").write_text(json.dumps(payload))

    summary = steps.summarize(run_dir, tmp_path / "out" / "summary.json")

    assert summary == {"run": run_dir.name, **parts, "passed": True}
    assert read(tmp_path / "out" / "summary.json") == summary


@pytest.mark.parametrize(
    "broken",
    [
        {"online": {"rows": 70}},
        {"retrieval": {"passed": False}},
        {"validation": {"documents": {}, "problems": ["bad"]}},
    ],
)
def test_summarize_fails_a_run_whose_stores_disagree_or_whose_checks_failed(tmp_path: Path, broken: dict) -> None:
    parts = {
        "validation": {"documents": {}, "problems": []},
        "embed": {"chunks": 71},
        "offline": {"rows": 71},
        "online": {"rows": 71},
        "retrieval": {"passed": True},
        **broken,
    }
    for name, payload in parts.items():
        (tmp_path / f"{name}.json").write_text(json.dumps(payload))

    assert steps.summarize(tmp_path, tmp_path / "summary.json")["passed"] is False


def test_summarize_after_a_failed_validation_has_only_what_ran(tmp_path: Path) -> None:
    (tmp_path / "validation.json").write_text(json.dumps({"documents": {}, "problems": ["no documents from handbook"]}))

    summary = steps.summarize(tmp_path, tmp_path / "summary.json")

    assert set(summary) == {"run", "validation", "passed"}
    assert summary["passed"] is False


def test_the_write_and_check_steps_save_what_they_did(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import fraudstream_rag.offline as offline
    import fraudstream_rag.online as online
    import fraudstream_rag.retrieval as retrieval

    prepared(tmp_path)
    steps.chunk(tmp_path)
    steps.embed_chunks(tmp_path, unit_vectors)
    monkeypatch.setattr(offline, "write_offline", lambda spark, frame: {"rows": len(frame)})
    monkeypatch.setattr(online, "write_online", lambda store, frame: {"rows": len(frame)})
    monkeypatch.setattr(retrieval, "check_golden", lambda store, golden, embed_texts: {"passed": True, "questions": len(golden.questions)})

    assert steps.write_offline_step(tmp_path, spark=None) == {"rows": 4}
    assert steps.write_online_step(tmp_path, store=None) == {"rows": 4}
    golden = Path(__file__).resolve().parents[1] / "checks" / "golden_questions.yaml"
    assert steps.check(tmp_path, None, golden) == {"passed": True, "questions": 12}
    assert [read(tmp_path / f"{name}.json") for name in ("offline", "online")] == [{"rows": 4}, {"rows": 4}]


def test_main_runs_each_step_with_its_dependencies(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import fraudstream_feast.spark as feast_spark
    import fraudstream_rag.sources as sources

    calls: list[str] = []
    spark = SimpleNamespace(stop=lambda: calls.append("spark stopped"))
    monkeypatch.setattr(steps, "_feature_store", lambda repo: f"store:{repo.name}")
    monkeypatch.setattr(steps, "load", lambda run_dir, **kwargs: calls.append(f"load {kwargs['feature_store']}") or {})
    monkeypatch.setattr(steps, "embed_chunks", lambda run_dir: calls.append("embed") or {})
    monkeypatch.setattr(steps, "write_offline_step", lambda run_dir, session: calls.append("offline") or {})
    monkeypatch.setattr(steps, "write_online_step", lambda run_dir, store: calls.append(f"online {store}") or {})
    monkeypatch.setattr(steps, "check", lambda run_dir, store, golden: calls.append(f"check {store}") or {"passed": True})
    monkeypatch.setattr(feast_spark, "configs_from_env", lambda: (None, None, None))
    # The steps read MinIO's settings from the environment, as they do in Airflow.
    monkeypatch.setenv("MINIO_ACCESS_KEY", "ak")
    monkeypatch.setenv("MINIO_SECRET_KEY", "sk")
    monkeypatch.setattr(feast_spark, "build_spark_session", lambda **kwargs: spark)
    monkeypatch.setattr(sources, "sync_handbook", lambda warehouse, source, target: calls.append(f"sync {target}") or {})
    run = ["--run-dir", str(tmp_path)]

    steps.main(["load", *run, "--handbook", "s3a://b/h", "--mlflow", "http://mlflow:5000", "--feast-repo", "feature_repo"])
    steps.main(["embed", *run])
    steps.main(["write-offline", *run])
    steps.main(["write-online", *run, "--repo", "rag_repo"])
    steps.main(["check", *run, "--repo", "rag_repo", "--golden", "g.yaml"])
    steps.main(["sync-handbook", "--source", "handbook", "--target", "s3a://fraudstream/rag/handbook"])

    assert calls == [
        "load store:feature_repo",
        "embed",
        "offline",
        "spark stopped",
        "online store:rag_repo",
        "check store:rag_repo",
        "sync s3a://fraudstream/rag/handbook",
    ]


def test_a_failed_golden_check_exits_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(steps, "_feature_store", lambda repo: None)
    monkeypatch.setattr(steps, "check", lambda run_dir, store, golden: {"passed": False})

    with pytest.raises(SystemExit) as exit_:
        steps.main(["check", "--run-dir", str(tmp_path), "--repo", "r", "--golden", "g.yaml"])

    assert exit_.value.code == 1


def test_main_chunks_and_summarizes_from_the_command_line(tmp_path: Path) -> None:
    prepared(tmp_path)

    steps.main(["chunk", "--run-dir", str(tmp_path)])
    steps.main(["summarize", "--run-dir", str(tmp_path), "--out", str(tmp_path / "summary.json")])

    assert len(read(tmp_path / "chunks.json")) == 4
    assert read(tmp_path / "summary.json")["passed"] is False


def test_json_files_are_indented_sorted_and_their_folders_made(tmp_path: Path) -> None:
    path = tmp_path / "a" / "b" / "c.json"

    steps._write(path, {"b": 2, "a": "é"})
    steps._write(path, {"b": 2, "a": "é"})

    assert path.read_text(encoding="utf-8") == '{\n  "a": "\\u00e9",\n  "b": 2\n}'


def test_embedding_no_chunks_reports_no_dimensions(tmp_path: Path) -> None:
    (tmp_path / "chunks.json").write_text("[]")

    summary = steps.embed_chunks(tmp_path, lambda texts: np.zeros((0, DIMENSIONS), dtype=np.float32))

    assert summary == {"model": MODEL_ID, "chunks": 0, "dimensions": [], "min_norm": None, "max_norm": None}


def test_norms_are_reported_to_four_places(tmp_path: Path) -> None:
    prepared(tmp_path)
    steps.chunk(tmp_path)

    def nearly_unit(texts: list[str]) -> np.ndarray:
        vectors = unit_vectors(texts)
        vectors[0, 0] = 0.99996
        vectors[1, 0] = 1.00004
        return vectors

    summary = steps.embed_chunks(tmp_path, nearly_unit)

    assert (summary["min_norm"], summary["max_norm"]) == (1.0, 1.0)
    assert summary["dimensions"] == [DIMENSIONS]


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["chunk"],
        ["load", "--run-dir", "r", "--mlflow", "m", "--feast-repo", "f"],
        ["load", "--run-dir", "r", "--handbook", "h", "--feast-repo", "f"],
        ["load", "--run-dir", "r", "--handbook", "h", "--mlflow", "m"],
        ["write-online", "--run-dir", "r"],
        ["check", "--run-dir", "r", "--golden", "g"],
        ["check", "--run-dir", "r", "--repo", "x"],
        ["summarize", "--run-dir", "r"],
        ["sync-handbook", "--target", "t"],
        ["sync-handbook", "--source", "s"],
    ],
)
def test_a_step_without_its_required_arguments_is_refused(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as exit_:
        steps.build_parser().parse_args(argv)

    assert exit_.value.code == 2
