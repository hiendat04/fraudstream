"""The pinned embedding model: the download refuses a wrong file, and every vector has length 1."""

from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pytest

from fraudstream_rag import embedding
from fraudstream_rag.embedding import BATCH_SIZE, DIMENSIONS, Embedder, fetch, model_dir


class FakeEncoding:
    def __init__(self) -> None:
        self.ids = [101, 2054, 102]
        self.attention_mask = [1, 1, 1]


class FakeTokenizer:
    def encode_batch(self, texts: list[str]) -> list[FakeEncoding]:
        return [FakeEncoding() for _ in texts]


class FakeSession:
    """Every token's vector is all 3s, so the normalised first-token vector is known."""

    def __init__(self) -> None:
        self.batches: list[int] = []

    def run(self, _outputs: None, feeds: dict[str, np.ndarray]) -> list[np.ndarray]:
        assert set(feeds) == {"input_ids", "attention_mask", "token_type_ids"}
        rows = feeds["input_ids"].shape[0]
        self.batches.append(rows)
        return [np.full((rows, 3, DIMENSIONS), 3.0, dtype=np.float32)]


def fake_embedder() -> tuple[Embedder, FakeSession]:
    session = FakeSession()
    return Embedder(FakeTokenizer(), session), session


def test_every_vector_has_length_one() -> None:
    vectors = fake_embedder()[0].embed(["card testing", "fraud rings"])

    assert vectors.shape == (2, DIMENSIONS)
    assert vectors.dtype == np.float32
    np.testing.assert_allclose(np.linalg.norm(vectors, axis=1), 1.0, rtol=1e-6)


@pytest.mark.parametrize(
    ("count", "batches"),
    [(1, [1]), (BATCH_SIZE, [BATCH_SIZE]), (BATCH_SIZE + 1, [BATCH_SIZE, 1])],
)
def test_texts_are_embedded_in_batches(count: int, batches: list[int]) -> None:
    embedder, session = fake_embedder()

    embedder.embed(["text"] * count)

    assert session.batches == batches


def test_no_texts_give_an_empty_matrix() -> None:
    assert fake_embedder()[0].embed([]).shape == (0, DIMENSIONS)


def test_the_model_folder_comes_from_the_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FRAUDSTREAM_EMBEDDING_DIR", str(tmp_path))

    assert model_dir() == tmp_path


def pin(monkeypatch: pytest.MonkeyPatch, content: bytes) -> None:
    monkeypatch.setattr(embedding, "FILES", {"tokenizer.json": hashlib.sha256(content).hexdigest()})


def serve(monkeypatch: pytest.MonkeyPatch, content: bytes) -> list[str]:
    urls: list[str] = []

    def urlretrieve(url: str, path: Path) -> None:
        urls.append(url)
        Path(path).write_bytes(content)

    monkeypatch.setattr(embedding.urllib.request, "urlretrieve", urlretrieve)
    return urls


def test_a_missing_file_is_downloaded_from_the_pinned_revision(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    pin(monkeypatch, b"right")
    urls = serve(monkeypatch, b"right")

    fetch(tmp_path)

    assert (tmp_path / "tokenizer.json").read_bytes() == b"right"
    assert urls == [f"https://huggingface.co/{embedding.REPOSITORY}/resolve/{embedding.REVISION}/tokenizer.json"]


def test_a_file_that_already_matches_is_not_downloaded_again(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    pin(monkeypatch, b"right")
    (tmp_path / "tokenizer.json").write_bytes(b"right")
    urls = serve(monkeypatch, b"right")

    fetch(tmp_path)

    assert urls == []


def test_a_download_that_does_not_match_is_refused_and_removed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    pin(monkeypatch, b"right")
    serve(monkeypatch, b"tampered")

    with pytest.raises(ValueError, match="does not match its pinned sha256"):
        fetch(tmp_path)

    assert list(tmp_path.iterdir()) == []


def test_loading_refuses_a_folder_without_the_pinned_files(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="is missing or is not"):
        Embedder.load(tmp_path)


def test_main_fetches_then_embeds_one_sentence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    fetched: list[Path] = []
    monkeypatch.setattr(embedding, "fetch", fetched.append)
    monkeypatch.setattr(Embedder, "load", classmethod(lambda cls, directory: fake_embedder()[0]))

    embedding.main([str(tmp_path), "--check"])

    assert fetched == [tmp_path]
    assert f"{DIMENSIONS} dimensions, norm 1.0000" in capsys.readouterr().out


def test_main_without_a_folder_fetches_into_the_configured_one(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    fetched: list[Path] = []
    monkeypatch.setenv("FRAUDSTREAM_EMBEDDING_DIR", str(tmp_path))
    monkeypatch.setattr(embedding, "fetch", fetched.append)

    embedding.main([])

    assert fetched == [tmp_path]
    assert capsys.readouterr().out == ""


@pytest.mark.skipif(not (model_dir() / "onnx/model.onnx").is_file(), reason="the model is not downloaded")
def test_the_real_model_puts_a_question_nearest_its_own_passage() -> None:
    embedding._embedder.cache_clear()
    vectors = embedding.embed(
        [
            "Many small payments on one card within a few minutes.",
            "Card testing: Signs\n\nMany payments on one card within minutes, often for small amounts.",
            "Handling customer data: Card numbers\n\nNever write, paste or send a full card number.",
        ]
    )

    assert vectors.shape == (3, DIMENSIONS)
    assert vectors[0] @ vectors[1] > vectors[0] @ vectors[2]
