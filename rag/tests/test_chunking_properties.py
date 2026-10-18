"""Properties of the chunker that must hold for any page Hypothesis invents."""

from __future__ import annotations

from hypothesis import given, strategies as st

from fraudstream_rag.chunking import MAX_WORDS, Document, chunk_document

LINE = st.one_of(
    st.builds(lambda level, text: f"{'#' * level} {text}", st.integers(1, 4), st.text("ab ", max_size=8)),
    st.text(alphabet="xyz ", max_size=40),
    st.just(""),
)
PAGES = st.lists(LINE, max_size=40).map("\n".join)


def chunks_of(text: str):
    return chunk_document(Document("handbook/page", "handbook", text), "m")


def body_words(text: str) -> list[str]:
    kept = [line for line in text.splitlines() if not line.startswith(("# ", "## ", "### "))]
    return " ".join(kept).split()


@given(PAGES)
def test_chunking_is_deterministic(text):
    assert chunks_of(text) == chunks_of(text)


@given(PAGES)
def test_chunk_ids_are_unique_within_a_document(text):
    ids = [chunk.chunk_id for chunk in chunks_of(text)]

    assert len(ids) == len(set(ids))


@given(PAGES)
def test_no_chunk_is_longer_than_max_words(text):
    assert all(len(chunk.text.split()) <= MAX_WORDS for chunk in chunks_of(text))


@given(PAGES)
def test_every_word_under_a_heading_lands_in_exactly_one_chunk(text):
    assert " ".join(chunk.text for chunk in chunks_of(text)).split() == body_words(text)
