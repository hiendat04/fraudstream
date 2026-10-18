"""The change plan, driven by CrossHair: after a store applies it, planning again finds nothing to do.

That is what makes a re-run safe, and a run that stopped half way finishable.
"""

from __future__ import annotations

from hypothesis import given, settings, strategies as st

from fraudstream_rag.changes import plan_changes

IDS = st.text(alphabet="abc", min_size=1, max_size=3)
HASHES = st.sampled_from(["h1", "h2", "h3"])
STORES = st.dictionaries(IDS, HASHES, max_size=6)


def applied(stored: dict[str, str], current: dict[str, str]) -> dict[str, str]:
    changes = plan_changes(stored, current)
    after = {chunk_id: digest for chunk_id, digest in stored.items() if chunk_id not in changes.delete}
    after.update({chunk_id: current[chunk_id] for chunk_id in changes.write})
    return after


@settings(backend="crosshair", max_examples=50, deadline=None)
@given(stored=STORES, current=STORES.filter(bool))
def test_after_applying_the_plan_there_is_nothing_left_to_do(stored, current):
    assert plan_changes(applied(stored, current), current).empty


@settings(backend="crosshair", max_examples=50, deadline=None)
@given(stored=STORES, current=STORES.filter(bool))
def test_applying_the_plan_leaves_exactly_the_current_chunks(stored, current):
    assert applied(stored, current) == current
