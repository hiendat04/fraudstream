from fraudstream_monitoring.summary import DriftSummary, should_retrain


def summary(status="stable", features=()):
    return DriftSummary(
        model_name="fraud-detection",
        model_version="2",
        window_start="2026-06-23",
        window_end="2026-06-30",
        rows=20000,
        status=status,
        features=list(features),
        available={"customer_features_available": 0.4},
    )


def test_survives_a_round_trip_through_json():
    original = summary("drift", [{"name": "amount", "psi": 0.61, "status": "drift"}])
    assert DriftSummary.from_json(original.to_json()) == original


def test_counts_inputs_by_band():
    one = summary("drift", [
        {"name": "amount", "psi": 0.61, "status": "drift"},
        {"name": "amount_sum_7d", "psi": 0.30, "status": "drift"},
        {"name": "event_hour", "psi": 0.12, "status": "warning"},
        {"name": "city_miami", "psi": 0.01, "status": "stable"},
    ])
    assert (one.count("drift"), one.count("warning"), one.count("stable")) == (2, 1, 1)


def test_only_drift_starts_a_retrain():
    assert should_retrain(summary("drift")) is True
    assert should_retrain(summary("warning")) is False
    assert should_retrain(summary("stable")) is False
    assert should_retrain(summary("not_enough_data")) is False


def test_force_starts_a_retrain_anyway():
    assert should_retrain(summary("stable"), force=True) is True
