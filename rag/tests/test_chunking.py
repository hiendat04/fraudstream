"""Chunking by heading: one chunk per section, ids from headings, hashes that change only when they should."""

from __future__ import annotations

import pytest

from fraudstream_rag.chunking import MAX_WORDS, Document, chunk_document, embedding_input, slug, title_of

MODEL = "model@1"
PAGE = """# Card testing

Card testing is checking stolen cards with small payments.

## Signs

- Many payments on one card within minutes.

## What to do

Hold every later payment on the card.
"""


def chunks_of(text: str, doc_id: str = "handbook/card-testing", model: str = MODEL):
    return chunk_document(Document(doc_id=doc_id, source="handbook", text=text), model)


def words(count: int, word: str = "word") -> str:
    return " ".join([word] * count)


def test_each_section_is_a_chunk_and_the_text_before_the_first_is_headed_by_the_title() -> None:
    chunks = chunks_of(PAGE)

    assert [chunk.chunk_id for chunk in chunks] == [
        "handbook/card-testing#card-testing",
        "handbook/card-testing#signs",
        "handbook/card-testing#what-to-do",
    ]
    assert [chunk.heading for chunk in chunks] == ["Card testing", "Signs", "What to do"]
    assert {chunk.title for chunk in chunks} == {"Card testing"}
    assert chunks[1].text == "- Many payments on one card within minutes."


def test_level_three_headings_start_sections_and_level_four_stays_in_the_text() -> None:
    chunks = chunks_of("# T\n\n### Steps\n\none\n\n#### Detail\n\ntwo\n")

    assert [chunk.heading for chunk in chunks] == ["Steps"]
    assert chunks[0].text == "one\n\n#### Detail\n\ntwo"


def test_a_second_level_one_heading_starts_a_section() -> None:
    assert [chunk.heading for chunk in chunks_of("# T\n\na\n\n# Appendix\n\nb\n")] == ["T", "Appendix"]


def test_a_heading_with_no_text_under_it_makes_no_chunk() -> None:
    assert [chunk.heading for chunk in chunks_of("# T\n\n## Empty\n\n## Full\n\ntext\n")] == ["Full"]


def test_a_repeated_heading_gets_a_numbered_id() -> None:
    ids = [chunk.chunk_id for chunk in chunks_of("# T\n\n## Steps\n\na\n\n## Steps\n\nb\n")]

    assert ids == ["handbook/card-testing#steps", "handbook/card-testing#steps-2"]


def test_a_document_without_a_title_is_titled_by_its_id() -> None:
    assert chunks_of("## Signs\n\ntext\n")[0].title == "handbook/card-testing"


def test_a_section_of_exactly_max_words_is_one_chunk() -> None:
    chunks = chunks_of(f"# T\n\n## Long\n\n{words(MAX_WORDS)}\n")

    assert len(chunks) == 1


def test_one_word_more_than_max_words_splits_between_paragraphs() -> None:
    chunks = chunks_of(f"# T\n\n## Long\n\n{words(MAX_WORDS)}\n\nlast\n")

    assert [chunk.chunk_id.split("#")[1] for chunk in chunks] == ["long", "long-part-2"]
    assert chunks[1].text == "last"


def test_a_paragraph_longer_than_max_words_is_cut_between_words() -> None:
    chunks = chunks_of(f"# T\n\n## Long\n\n{words(2 * MAX_WORDS + 1)}\n")

    assert [len(chunk.text.split()) for chunk in chunks] == [MAX_WORDS, MAX_WORDS, 1]


def test_short_paragraphs_share_a_chunk() -> None:
    assert chunks_of("# T\n\n## S\n\none\n\ntwo\n")[0].text == "one\n\ntwo"


def test_the_hash_covers_the_embedding_model() -> None:
    assert chunks_of(PAGE, model="model@1")[1].content_hash != chunks_of(PAGE, model="model@2")[1].content_hash


def test_the_hash_covers_the_title_that_is_embedded_with_the_text() -> None:
    renamed = PAGE.replace("# Card testing", "# Card testing (retired)")

    assert chunks_of(PAGE)[1].content_hash != chunks_of(renamed)[1].content_hash


def test_editing_one_section_changes_only_that_chunks_hash() -> None:
    before = {chunk.chunk_id: chunk.content_hash for chunk in chunks_of(PAGE)}
    after = {chunk.chunk_id: chunk.content_hash for chunk in chunks_of(PAGE.replace("Hold every", "Block every"))}

    assert [chunk_id for chunk_id in before if before[chunk_id] != after[chunk_id]] == ["handbook/card-testing#what-to-do"]


def test_adding_a_section_moves_no_other_chunk_id() -> None:
    before = [chunk.chunk_id for chunk in chunks_of(PAGE)]
    after = [chunk.chunk_id for chunk in chunks_of(PAGE.replace("## Signs", "## Why it matters\n\nLosses.\n\n## Signs"))]

    assert set(before) <= set(after)


def test_what_is_embedded_puts_the_title_and_heading_first() -> None:
    assert embedding_input("Card testing", "Signs", "text") == "Card testing: Signs\n\ntext"


@pytest.mark.parametrize(
    ("heading", "expected"),
    [("What to do?", "what-to-do"), ("3-D Secure", "3-d-secure"), ("", "section"), ("!!!", "section")],
)
def test_slugs_are_lower_case_words_joined_by_dashes(heading: str, expected: str) -> None:
    assert slug(heading) == expected


def test_the_title_is_the_first_level_one_heading() -> None:
    assert title_of("intro\n## Not this\n# This one\n# Nor this\n") == "This one"
    assert title_of("no headings") == ""
