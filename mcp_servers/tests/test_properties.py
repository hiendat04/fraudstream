"""Properties that must hold for any input Hypothesis invents.

CrossHair drives outcome(), where the answer turns on integer comparisons and it
solves the boundaries instead of guessing them. detail() takes any JSON at all,
which random search covers better.
"""

import httpx
from hypothesis import given, settings, strategies as st

from fraudstream_mcp.common import FAILED, NOT_FOUND, OK, REJECTED, detail, outcome

JSON = st.recursive(
    st.none() | st.booleans() | st.integers() | st.text(max_size=20),
    lambda children: st.lists(children, max_size=4) | st.dictionaries(st.text(max_size=8), children, max_size=4),
    max_leaves=12,
)


@settings(backend="crosshair", max_examples=50, deadline=None)
@given(status=st.integers(min_value=0, max_value=1000))
def test_every_status_has_exactly_one_outcome_and_only_2xx_is_ok(status):
    found = outcome(status)
    assert found in (OK, NOT_FOUND, REJECTED, FAILED)
    assert found == outcome(status)
    assert (found == OK) == (200 <= status < 300)
    assert (found == NOT_FOUND) == (status == 404)


@given(body=JSON)
def test_detail_never_raises_and_gives_the_same_line_twice(body):
    response = httpx.Response(500, json=body)
    first = detail(response)
    assert isinstance(first, str)
    assert first == detail(response)
