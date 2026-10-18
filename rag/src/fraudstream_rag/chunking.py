"""Split a Markdown document into chunks, one per section.

The first level-1 heading is the document's title. Every other heading up to level
3 starts a section, and the text before the first one is headed by the title. A
section longer than MAX_WORDS is split between paragraphs.

A chunk's id comes from its heading, not its position, so adding a section moves no
other chunk's id. Its hash covers the embedding model and the exact text that is
embedded: change either, and the chunk is embedded and written again.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

MAX_WORDS = 300
_HEADING = re.compile(r"^(#{1,3})\s+(.*?)\s*$")


@dataclass(frozen=True)
class Document:
    doc_id: str
    source: str
    text: str


@dataclass(frozen=True)
class Chunk:
    chunk_id: str
    doc_id: str
    source: str
    title: str
    heading: str
    text: str
    content_hash: str


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-") or "section"


def title_of(text: str) -> str:
    for line in text.splitlines():
        match = _HEADING.match(line)
        if match and len(match.group(1)) == 1:
            return match.group(2)
    return ""


def embedding_input(title: str, heading: str, text: str) -> str:
    """What is embedded: the title and heading in front, so a "What to do" section knows its topic."""

    return f"{title}: {heading}\n\n{text}"


def chunk_document(document: Document, model_id: str) -> list[Chunk]:
    title = title_of(document.text) or document.doc_id
    chunks: list[Chunk] = []
    used: set[str] = set()
    for heading, body in _sections(document.text):
        for part, text in enumerate(_pieces(body), start=1):
            base = slug(heading) if part == 1 else f"{slug(heading)}-part-{part}"
            embedded = embedding_input(title, heading, text)
            chunks.append(
                Chunk(
                    chunk_id=_unique(f"{document.doc_id}#{base}", used),
                    doc_id=document.doc_id,
                    source=document.source,
                    title=title,
                    heading=heading,
                    text=text,
                    content_hash=hashlib.sha256(f"{model_id}\n{embedded}".encode()).hexdigest(),
                )
            )
    return chunks


def _sections(text: str) -> list[tuple[str, str]]:
    title = title_of(text)
    heading, lines, sections = title, [], []
    seen_title = False
    for line in text.splitlines():
        match = _HEADING.match(line)
        if match and len(match.group(1)) == 1 and not seen_title:
            seen_title = True
            continue
        if match:
            sections.append((heading, "\n".join(lines).strip()))
            heading, lines = match.group(2), []
            continue
        lines.append(line)
    sections.append((heading, "\n".join(lines).strip()))
    return [(heading, body) for heading, body in sections if body]


def _pieces(body: str) -> list[str]:
    """Split a section between paragraphs, so no piece is longer than MAX_WORDS words."""

    pieces: list[str] = []
    current: list[str] = []
    size = 0
    for paragraph in re.split(r"\n\s*\n", body):
        words = paragraph.split()
        if size and size + len(words) > MAX_WORDS:
            pieces.append("\n\n".join(current))
            current, size = [], 0
        while len(words) > MAX_WORDS:
            pieces.append(" ".join(words[:MAX_WORDS]))
            words = words[MAX_WORDS:]
            paragraph = " ".join(words)
        current.append(paragraph.strip())
        size += len(words)
    pieces.append("\n\n".join(current))
    return pieces


def _unique(candidate: str, used: set[str]) -> str:
    chunk_id, n = candidate, 2
    while chunk_id in used:
        chunk_id = f"{candidate}-{n}"
        n += 1
    used.add(chunk_id)
    return chunk_id
