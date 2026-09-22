"""Properties that must hold for any input Hypothesis invents.

CrossHair drives the TTL rule, where the answer turns on one comparison and it
solves the boundary instead of guessing it. The rest is NumPy or pandas, which
CrossHair cannot see into, so those run on ordinary random search.
"""

from datetime import UTC, datetime, timedelta

import numpy as np
from hypothesis import given, settings, strategies as st

from fraudstream_ml.dataset import BATCH_FEATURE_VIEWS

from fraudstream_api.drift_detection.psi import bin_counts, psi
from fraudstream_api.drift_detection.reference import Reference, build_reference
from fraudstream_api.inference.features import OnlineFeatures, model_inputs, usable_features

from tests.test_inference_app import TTLS
from tests.test_inference_features import WITH_HISTORY, transaction_from

PAID_AT = datetime(2026, 6, 30, 20, 0, tzinfo=UTC)
VIEW = "customer_rolling_features"
NAMES = [name for names in BATCH_FEATURE_VIEWS.values() for name in names]
AGES = st.integers(min_value=-5_000_000, max_value=15_000_000)
VALUES = st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False)


def stored(age_seconds: int) -> OnlineFeatures:
    return OnlineFeatures(
        values={name: 1.0 for name in NAMES},
        event_times={view: PAID_AT - timedelta(seconds=age_seconds) for view in TTLS},
    )


@settings(backend="crosshair", max_examples=50, deadline=None)
@given(age_seconds=AGES)
def test_applying_the_ttl_rule_twice_is_the_same_as_once(age_seconds):
    online = stored(age_seconds)

    once = usable_features(online, PAID_AT, TTLS)
    twice = usable_features(
        OnlineFeatures(values=once, event_times=online.event_times), PAID_AT, TTLS
    )

    assert once == twice


@settings(backend="crosshair", max_examples=50, deadline=None)
@given(age_seconds=AGES)
def test_history_is_kept_exactly_when_it_is_neither_late_nor_too_old(age_seconds):
    usable = usable_features(stored(age_seconds), PAID_AT, TTLS)

    kept = usable[BATCH_FEATURE_VIEWS[VIEW][0]] is not None
    assert kept == (0 <= age_seconds <= TTLS[VIEW].total_seconds())


@given(order=st.permutations(NAMES))
def test_the_same_payment_gives_the_same_inputs_whatever_the_feature_order(order):
    transaction = transaction_from(WITH_HISTORY)
    features = {name: WITH_HISTORY[name] for name in NAMES}
    shuffled = {name: features[name] for name in order}

    assert model_inputs(transaction, shuffled) == model_inputs(transaction, features)


@given(shares=st.lists(st.floats(min_value=0.0, max_value=1.0), min_size=2, max_size=12))
def test_a_distribution_has_no_drift_against_itself(shares):
    counts = np.array(shares)

    assert psi(counts, counts) == 0.0


@given(
    values=st.lists(st.one_of(VALUES, st.just(np.nan)), min_size=1, max_size=50),
    edges=st.lists(VALUES, min_size=1, max_size=9, unique=True),
)
def test_every_value_lands_in_exactly_one_bin(values, edges):
    counts = bin_counts(np.array(values), np.array(sorted(edges)))

    assert counts.sum() == len(values)


@given(columns=st.lists(VALUES, min_size=20, max_size=200))
def test_a_reference_survives_a_round_trip_through_json(columns):
    reference = build_reference(
        {"amount": np.array(columns)},
        model_name="fraud-detection",
        model_version="2",
        data_snapshot_id="3023861480409485916",
    )

    written = reference.to_json()

    assert Reference.from_json(written).to_json() == written
