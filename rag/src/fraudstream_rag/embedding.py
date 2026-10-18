"""The embedding model, pinned once for the RAG pipeline and the context API.

Documents and questions must be embedded by exactly the same model. Otherwise, a
search compares vectors from two different spaces and returns the wrong chunks
without any error. So the model is named here, by repository, revision and file
hashes, and both sides import this module.

BAAI/bge-base-en-v1.5: 768 dimensions, 512 tokens, about 436 MB as ONNX, run with
onnxruntime on the CPU. Questions are embedded the same way as documents: on the
golden questions, its query instruction made no difference.

    python -m fraudstream_rag.embedding /opt/models/bge-base-en-v1.5 --check
"""

from __future__ import annotations

import argparse
import hashlib
import os
import urllib.request
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np

REPOSITORY = "BAAI/bge-base-en-v1.5"
REVISION = "a5beb1e3e68b9ab74eb54cfd186867f64f240e1a"
FILES = {
    "onnx/model.onnx": "9bc579acdba21c253c62a9bf866891355a63ffa3442b52c8a37d75b2ccb91848",
    "tokenizer.json": "d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66",
}
MODEL_ID = f"{REPOSITORY}@{REVISION}"
DIMENSIONS = 768
MAX_TOKENS = 512
BATCH_SIZE = 32
DEFAULT_DIR = Path.home() / ".cache" / "fraudstream" / "bge-base-en-v1.5"


def model_dir() -> Path:
    return Path(os.environ.get("FRAUDSTREAM_EMBEDDING_DIR", DEFAULT_DIR))


def fetch(directory: Path) -> None:
    """Download each pinned file that is missing or wrong, and refuse any that does not match its hash."""

    for name, sha256 in FILES.items():
        path = directory / name
        if path.is_file() and _sha256(path) == sha256:
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_name(path.name + ".part")
        url = f"https://huggingface.co/{REPOSITORY}/resolve/{REVISION}/{name}"
        urllib.request.urlretrieve(url, partial)
        if _sha256(partial) != sha256:
            partial.unlink()
            raise ValueError(f"{url} does not match its pinned sha256")
        partial.replace(path)


class Embedder:
    """Turns texts into unit-length vectors: bge's first-token vector, normalised."""

    def __init__(self, tokenizer: Any, session: Any) -> None:
        self._tokenizer = tokenizer
        self._session = session

    @classmethod
    def load(cls, directory: Path) -> "Embedder":
        for name, sha256 in FILES.items():
            path = directory / name
            if not path.is_file() or _sha256(path) != sha256:
                raise FileNotFoundError(
                    f"{path} is missing or is not {MODEL_ID}: "
                    f"run python -m fraudstream_rag.embedding {directory}"
                )
        import onnxruntime
        from tokenizers import Tokenizer

        tokenizer = Tokenizer.from_file(str(directory / "tokenizer.json"))
        tokenizer.enable_truncation(max_length=MAX_TOKENS)
        tokenizer.enable_padding()
        session = onnxruntime.InferenceSession(
            str(directory / "onnx/model.onnx"), providers=["CPUExecutionProvider"]
        )
        return cls(tokenizer, session)

    def embed(self, texts: list[str]) -> np.ndarray:
        batches = [self._batch(texts[i : i + BATCH_SIZE]) for i in range(0, len(texts), BATCH_SIZE)]
        return np.vstack(batches) if batches else np.zeros((0, DIMENSIONS), dtype=np.float32)

    def _batch(self, texts: list[str]) -> np.ndarray:
        encoded = self._tokenizer.encode_batch(texts)
        ids = np.array([item.ids for item in encoded], dtype=np.int64)
        mask = np.array([item.attention_mask for item in encoded], dtype=np.int64)
        hidden = self._session.run(
            None, {"input_ids": ids, "attention_mask": mask, "token_type_ids": np.zeros_like(ids)}
        )[0]
        first = hidden[:, 0]
        return (first / np.linalg.norm(first, axis=1, keepdims=True)).astype(np.float32)


def embed(texts: list[str]) -> np.ndarray:
    """One row of DIMENSIONS floats, of length 1, per text."""

    return _embedder().embed(texts)


@lru_cache(maxsize=1)
def _embedder() -> Embedder:
    return Embedder.load(model_dir())


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Download the pinned embedding model.")
    parser.add_argument("directory", type=Path, nargs="?", default=None)
    parser.add_argument("--check", action="store_true", help="also embed one sentence")
    arguments = parser.parse_args(argv)
    directory = arguments.directory or model_dir()
    fetch(directory)
    if arguments.check:
        vector = Embedder.load(directory).embed(["Many small payments on one card in a few minutes."])
        assert vector.shape == (1, DIMENSIONS), vector.shape
        print(f"{MODEL_ID}: {DIMENSIONS} dimensions, norm {float(np.linalg.norm(vector)):.4f}")


if __name__ == "__main__":
    main()
