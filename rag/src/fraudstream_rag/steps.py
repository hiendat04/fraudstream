"""The RAG pipeline's steps, one command each, as the Airflow tasks run them.

Each step reads what the step before it left in the run folder and leaves its own
file there, so a failed task can be retried on its own:

    load            documents.json   the handbook, the model cards, the feature dictionary
    validate        validation.json  exits 1 when a document is not safe to index
    chunk           chunks.json
    embed           chunks.parquet   every chunk with its vector; embed.json
    write-offline   offline.json     the Iceberg table
    write-online    online.json      Feast's pgvector store, changed chunks only
    check           retrieval.json   the golden questions through Feast; exits 1 below the bar
    summarize       the run in one file, which DataHub's checks read

    python -m fraudstream_rag.steps chunk --run-dir data/rag/runs/manual
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from fraudstream_rag.chunking import Chunk, Document, chunk_document, embedding_input
from fraudstream_rag.documents import SOURCES, validate
from fraudstream_rag.embedding import MODEL_ID, embed

PARTS = ("validation", "embed", "offline", "online", "retrieval")


def load(run_dir: Path, *, warehouse: Any, handbook_uri: str, mlflow: Any, feature_store: Any) -> dict[str, int]:
    from fraudstream_rag.sources import feature_dictionary_documents, handbook_documents, model_card_documents

    documents = (
        handbook_documents(warehouse, handbook_uri)
        + model_card_documents(mlflow)
        + feature_dictionary_documents(feature_store)
    )
    _write(run_dir / "documents.json", [asdict(document) for document in documents])
    return _by_source(documents)


def validate_documents(run_dir: Path) -> list[str]:
    documents = _documents(run_dir)
    problems = validate(documents)
    _write(run_dir / "validation.json", {"documents": _by_source(documents), "problems": problems})
    return problems


def chunk(run_dir: Path) -> int:
    chunks = [
        piece
        for document in _documents(run_dir)
        for piece in chunk_document(document, MODEL_ID)
    ]
    _write(run_dir / "chunks.json", [asdict(piece) for piece in chunks])
    return len(chunks)


def embed_chunks(
    run_dir: Path,
    embed_texts: Callable[[list[str]], np.ndarray] = embed,
    now: datetime | None = None,
) -> dict[str, Any]:
    chunks = [Chunk(**item) for item in _read(run_dir / "chunks.json")]
    vectors = embed_texts([embedding_input(piece.title, piece.heading, piece.text) for piece in chunks])
    frame = pd.DataFrame([asdict(piece) for piece in chunks], columns=[field.name for field in fields(Chunk)])
    frame["embedding"] = list(vectors)
    frame["updated_at"] = pd.Timestamp(now or datetime.now(UTC))
    frame.to_parquet(run_dir / "chunks.parquet", index=False)
    norms = np.linalg.norm(vectors, axis=1)
    summary = {
        "model": MODEL_ID,
        "chunks": len(chunks),
        "dimensions": sorted({int(vector.shape[0]) for vector in vectors}),
        "min_norm": round(float(norms.min()), 4) if len(norms) else None,
        "max_norm": round(float(norms.max()), 4) if len(norms) else None,
    }
    _write(run_dir / "embed.json", summary)
    return summary


def write_offline_step(run_dir: Path, spark: Any) -> dict[str, Any]:
    from fraudstream_rag.offline import write_offline

    summary = write_offline(spark, pd.read_parquet(run_dir / "chunks.parquet"))
    _write(run_dir / "offline.json", summary)
    return summary


def write_online_step(run_dir: Path, store: Any) -> dict[str, Any]:
    from fraudstream_rag.online import write_online

    summary = write_online(store, pd.read_parquet(run_dir / "chunks.parquet"))
    _write(run_dir / "online.json", summary)
    return summary


def check(run_dir: Path, store: Any, golden_path: Path, embed_texts: Callable[[list[str]], np.ndarray] = embed) -> dict[str, Any]:
    from fraudstream_rag.retrieval import check_golden, load_golden

    summary = check_golden(store, load_golden(golden_path), embed_texts)
    _write(run_dir / "retrieval.json", summary)
    return summary


def summarize(run_dir: Path, out: Path) -> dict[str, Any]:
    """Whatever the run produced, in one file; a step that never ran is simply missing."""

    summary: dict[str, Any] = {"run": run_dir.name}
    for part in PARTS:
        path = run_dir / f"{part}.json"
        if path.is_file():
            summary[part] = _read(path)
    chunks = summary.get("embed", {}).get("chunks")
    summary["passed"] = (
        "validation" in summary
        and not summary["validation"]["problems"]
        and summary.get("retrieval", {}).get("passed") is True
        and bool(chunks)
        and summary.get("offline", {}).get("rows") == chunks
        and summary.get("online", {}).get("rows") == chunks
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    _write(out, summary)
    return summary


def _documents(run_dir: Path) -> list[Document]:
    return [Document(**item) for item in _read(run_dir / "documents.json")]


def _by_source(documents: list[Document]) -> dict[str, int]:
    return {source: sum(document.source == source for document in documents) for source in SOURCES}


def _read(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _feature_store(repo: Path) -> Any:
    from feast import FeatureStore

    return FeatureStore(repo_path=str(repo))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    steps = parser.add_subparsers(dest="step", required=True)
    for name in ("load", "validate", "chunk", "embed", "write-offline", "write-online", "check", "summarize"):
        step = steps.add_parser(name)
        step.add_argument("--run-dir", type=Path, required=True)
        if name == "load":
            step.add_argument("--handbook", required=True, help="s3a://fraudstream/rag/handbook")
            step.add_argument("--mlflow", required=True, help="http://mlflow:5000")
            step.add_argument("--feast-repo", type=Path, required=True, help="the fraudstream Feast repo")
        if name in ("write-online", "check"):
            step.add_argument("--repo", type=Path, required=True, help="the fraudstream_rag Feast repo")
        if name == "check":
            step.add_argument("--golden-question", type=Path, required=True)
        if name == "summarize":
            step.add_argument("--out", type=Path, required=True)
    sync = steps.add_parser("sync-handbook")
    sync.add_argument("--source", type=Path, required=True)
    sync.add_argument("--target", required=True)
    return parser


def main(argv: list[str] | None = None) -> None:
    arguments = build_parser().parse_args(argv)
    failed = False
    if arguments.step == "load":
        import httpx

        from fraudstream.jobs.warehouse import WarehouseConfig

        with httpx.Client(base_url=arguments.mlflow, timeout=30) as mlflow:
            result: Any = load(
                arguments.run_dir,
                warehouse=WarehouseConfig.from_env(),
                handbook_uri=arguments.handbook,
                mlflow=mlflow,
                feature_store=_feature_store(arguments.feast_repo),
            )
    elif arguments.step == "validate":
        result = validate_documents(arguments.run_dir)
        failed = bool(result)
    elif arguments.step == "chunk":
        result = chunk(arguments.run_dir)
    elif arguments.step == "embed":
        result = embed_chunks(arguments.run_dir)
    elif arguments.step == "write-offline":
        from fraudstream_feast.spark import build_spark_session, configs_from_env

        warehouse, iceberg, postgres = configs_from_env()
        spark = build_spark_session(warehouse=warehouse, iceberg=iceberg, postgres=postgres, app_name="FraudStreamRagOffline")
        try:
            result = write_offline_step(arguments.run_dir, spark)
        finally:
            spark.stop()
    elif arguments.step == "write-online":
        result = write_online_step(arguments.run_dir, _feature_store(arguments.repo))
    elif arguments.step == "check":
        result = check(arguments.run_dir, _feature_store(arguments.repo), arguments.golden_question)
        failed = not result["passed"]
    elif arguments.step == "summarize":
        result = summarize(arguments.run_dir, arguments.out)
    else:
        from fraudstream.jobs.warehouse import WarehouseConfig
        from fraudstream_rag.sources import sync_handbook

        result = sync_handbook(WarehouseConfig.from_env(), arguments.source, arguments.target)
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
