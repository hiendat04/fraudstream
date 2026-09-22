"""The values on and beside each boundary the APIs enforce.

The classes inside and outside each rule already have their own tests. These
cases sit on the edges, where an off-by-one would hide. Payment and batch
limits go through the real HTTP endpoints.
"""

from datetime import UTC, datetime, timedelta

import numpy as np
import pytest

from fraudstream_ml.dataset import BATCH_FEATURE_VIEWS

from fraudstream_api.drift_detection.psi import bin_counts, bin_edges
from fraudstream_api.inference.features import OnlineFeatures, usable_features

from tests.test_drift_app import rows
from tests.test_inference_app import TTLS

SECOND = timedelta(seconds=1)


@pytest.mark.parametrize(
    ("field", "value", "status"),
    [
        pytest.param("amount", -0.01, 422, id="amount-below-zero-is-refused"),
        pytest.param("amount", 0, 422, id="amount-zero-is-refused"),
        pytest.param("amount", 0.01, 200, id="amount-just-above-zero-is-scored"),
        *[
            pytest.param(field, "x" * length, status, id=f"{field}-{length}-chars")
            for field in ("transaction_id", "customer_id", "merchant_id")
            for length, status in ((0, 422), (1, 200), (64, 200), (65, 422))
        ],
        pytest.param("channel", "ATM", 422, id="channel-other-case-is-refused"),
        pytest.param("channel", " atm", 422, id="channel-padded-is-refused"),
        pytest.param("city", "New York ", 422, id="city-padded-is-refused"),
    ],
)
def test_payment_limits(inference_client, payment, field, value, status):
    payment[field] = value

    assert inference_client.post("/v1/predict", json=payment).status_code == status


@pytest.mark.parametrize(
    ("ahead", "status"),
    [
        # Ten seconds from the edge either way, because the check reads the clock.
        pytest.param(timedelta(minutes=4, seconds=50), 200, id="just-inside-clock-skew"),
        pytest.param(timedelta(minutes=5, seconds=10), 422, id="just-past-clock-skew"),
    ],
)
def test_how_far_ahead_a_payment_may_be(inference_client, payment, ahead, status):
    payment["event_timestamp"] = (datetime.now(UTC) + ahead).isoformat()

    assert inference_client.post("/v1/predict", json=payment).status_code == status


@pytest.mark.parametrize(
    ("view", "age", "kept"),
    [
        pytest.param(view, age, kept, id=f"{view}-{label}")
        for view in ("customer_rolling_features", "customer_orders_90d_features")
        for label, age, kept in (
            ("stamped-after-the-payment", -SECOND, False),
            ("stamped-on-the-payment", timedelta(0), True),
            ("exactly-the-ttl-old", TTLS[view], True),
            ("one-second-past-the-ttl", TTLS[view] + SECOND, False),
        )
    ],
)
def test_the_ttl_edges(view, age, kept):
    paid_at = datetime(2026, 6, 30, 20, 0, tzinfo=UTC)
    names = BATCH_FEATURE_VIEWS[view]
    online = OnlineFeatures(
        values={name: 1.0 for name in names}, event_times={view: paid_at - age}
    )

    usable = usable_features(online, paid_at, TTLS)

    assert all((usable[name] is not None) == kept for name in names)


@pytest.mark.parametrize(
    ("distinct", "expected_edges"),
    [
        pytest.param(10, np.arange(9) + 0.5, id="ten-values-get-one-bin-each"),
        pytest.param(11, np.arange(1.0, 10.0), id="eleven-values-get-deciles"),
    ],
)
def test_the_one_bin_per_value_cut_off(distinct, expected_edges):
    values = np.repeat(np.arange(float(distinct)), 5)

    np.testing.assert_allclose(bin_edges(values), expected_edges)


@pytest.mark.parametrize(
    ("value", "expected_counts"),
    [
        pytest.param(0.5, [1, 0, 0, 0], id="below-the-first-edge"),
        pytest.param(1.0, [0, 1, 0, 0], id="exactly-on-an-edge-goes-above-it"),
        pytest.param(2.0, [0, 0, 1, 0], id="exactly-on-the-last-edge"),
        pytest.param(np.nan, [0, 0, 0, 1], id="missing"),
    ],
)
def test_a_value_against_the_bin_edges(value, expected_counts):
    counts = bin_counts(np.array([value]), np.array([1.0, 2.0]))

    assert counts.tolist() == expected_counts


@pytest.mark.parametrize(
    ("size", "status"),
    [
        pytest.param(0, 422, id="empty-batch-is-refused"),
        pytest.param(1, 200, id="one-row"),
        pytest.param(5000, 200, id="the-largest-batch"),
        pytest.param(5001, 422, id="one-row-too-many"),
    ],
)
def test_batch_size_limits(drift_client, size, status):
    answer = drift_client.post("/v1/observations", json={"observations": rows(size)})

    assert answer.status_code == status


@pytest.mark.parametrize(
    ("observations", "has_verdict"),
    [
        pytest.param(49, False, id="one-short-of-the-minimum"),
        pytest.param(50, True, id="exactly-the-minimum"),
    ],
)
def test_the_observation_minimum(drift_client, observations, has_verdict):
    drift_client.post("/v1/observations", json={"observations": rows(observations)})

    status = drift_client.get("/v1/drift").json()["status"]

    assert (status != "not_enough_data") == has_verdict
