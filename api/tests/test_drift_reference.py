"""Tests for the reference profile and the comparison against it."""

import unittest

import numpy as np

from fraudstream_api.drift_detection.psi import bin_counts
from fraudstream_api.drift_detection.reference import Reference, build_reference, compare

ROWS = 1000


def columns(shift: float = 0.0) -> dict[str, np.ndarray]:
    """Three inputs of the kinds the model has: continuous, yes/no, and one with gaps."""

    rng = np.random.default_rng(0)
    amount = rng.normal(100.0, 20.0, ROWS) + shift
    flag = np.array([0.0] * (ROWS - 50) + [1.0] * 50)
    sparse = np.where(rng.random(ROWS) < 0.5, rng.normal(5.0, 1.0, ROWS), np.nan)
    return {"amount": amount, "customer_features_available": flag, "txn_count_7d": sparse}


def reference_of(shift: float = 0.0) -> Reference:
    return build_reference(
        columns(shift),
        model_name="fraud-detection",
        model_version="2",
        data_snapshot_id="3023861480409485916",
    )


class ReferenceTest(unittest.TestCase):
    def test_it_records_the_model_and_the_data_it_describes(self):
        reference = reference_of()

        self.assertEqual("fraud-detection", reference.model_name)
        self.assertEqual("2", reference.model_version)
        self.assertEqual("3023861480409485916", reference.data_snapshot_id)
        self.assertEqual(ROWS, reference.rows)

    def test_proportions_sum_to_one_for_every_feature(self):
        reference = reference_of()

        for name, feature in reference.features.items():
            with self.subTest(feature=name):
                self.assertAlmostEqual(1.0, float(feature.proportions.sum()), places=9)

    def test_it_keeps_the_model_input_order(self):
        reference = reference_of()

        self.assertEqual(list(columns()), list(reference.features))

    def test_it_survives_a_round_trip_through_json(self):
        reference = reference_of()

        restored = Reference.from_json(reference.to_json())

        self.assertEqual(reference.model_version, restored.model_version)
        self.assertEqual(list(reference.features), list(restored.features))
        for name, feature in reference.features.items():
            with self.subTest(feature=name):
                np.testing.assert_allclose(feature.edges, restored.features[name].edges)
                np.testing.assert_allclose(feature.proportions, restored.features[name].proportions)


class CompareTest(unittest.TestCase):
    def counts_for(self, reference: Reference, data: dict[str, np.ndarray]):
        return {name: bin_counts(data[name], reference.features[name].edges) for name in data}

    def test_the_same_data_reads_as_stable(self):
        reference = reference_of()

        drifts = compare(reference, self.counts_for(reference, columns()), ROWS)

        for drift in drifts:
            with self.subTest(feature=drift.name):
                self.assertEqual(0.0, drift.psi)
                self.assertEqual("stable", drift.status)

    def test_compare_puts_the_shifted_feature_first(self):
        reference = reference_of()

        drifts = compare(reference, self.counts_for(reference, columns(shift=40.0)), ROWS)

        self.assertEqual("amount", drifts[0].name)
        self.assertEqual("drift", drifts[0].status)
        self.assertEqual("stable", drifts[-1].status)

    def test_it_reports_every_input(self):
        reference = reference_of()

        drifts = compare(reference, self.counts_for(reference, columns()), ROWS)

        self.assertEqual(sorted(columns()), sorted(drift.name for drift in drifts))


if __name__ == "__main__":
    unittest.main()
