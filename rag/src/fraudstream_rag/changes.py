"""What a store must change to hold exactly the current chunks.

Both stores keep one row per chunk id with the hash of what was embedded. A chunk
is written when its id is new or its hash differs, and deleted when its id is gone.
The plan is made against what the store holds now, not against the last run, so a
run that stopped half way is finished by the next one.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class Changes:
    write: tuple[str, ...]
    delete: tuple[str, ...]
    unchanged: int

    @property
    def empty(self) -> bool:
        return not self.write and not self.delete


def plan_changes(stored: Mapping[str, str], current: Mapping[str, str]) -> Changes:
    if stored and not current:
        raise ValueError("no current chunks: refusing to delete every stored chunk")
    write = tuple(sorted(chunk_id for chunk_id, digest in current.items() if stored.get(chunk_id) != digest))
    delete = tuple(sorted(set(stored) - set(current)))
    return Changes(write=write, delete=delete, unchanged=len(current) - len(write))
