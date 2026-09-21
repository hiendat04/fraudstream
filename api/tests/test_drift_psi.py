"""Tests for the binning and the drift score.

These rules decide every number the drift API reports, so they are pinned here.
"""

import unittest

import numpy as np

from fraudstream_api.drift_detection.psi import bin_counts, bin_edges, psi, status


class BinningTest(unittest.TestCase):
    def test_a_continuous_feature_gets_ten_equal_bins(self):
        values = np.arange(1000.0)

        counts = bin_counts(values, bin_edges(values))

        self.assertEqual(11, len(counts))
        self.assertEqual([100] * 10, list(counts[:10]))
        self.assertEqual(0, counts[-1])

    def test_a_rare_yes_no_feature_still_gets_two_bins(self):
        """Nineteen of the model's 27 yes/no inputs are 1 in under 10% of rows.

        Every decile of those is 0, so decile bins would put the 0s and the 1s
        in one bin and the drift API could never see them change.
        """

        values = np.array([0.0] * 950 + [1.0] * 50)

        counts = bin_counts(values, bin_edges(values))

        self.assertEqual([950, 50, 0], list(counts))

    def test_missing_values_get_their_own_bin(self):
        reference = np.arange(100.0)
        edges = bin_edges(reference)

        counts = bin_counts(np.array([1.0, np.nan, 2.0, np.nan]), edges)

        self.assertEqual(2, counts[-1])
        self.assertEqual(4, counts.sum())

    def test_a_feature_the_store_never_has_is_all_missing(self):
        values = np.array([np.nan, np.nan, np.nan])

        counts = bin_counts(values, bin_edges(values))

        self.assertEqual(3, counts[-1])


class PsiTest(unittest.TestCase):
    def test_identical_distributions_have_zero_psi(self):
        shares = np.array([0.2, 0.3, 0.5])

        self.assertEqual(0.0, psi(shares, shares))

    def test_psi_matches_a_value_worked_out_by_hand(self):
        """0.4 * ln(1.8) + (-0.4) * ln(0.2) = 0.878890."""

        value = psi(np.array([0.5, 0.5]), np.array([0.9, 0.1]))

        self.assertAlmostEqual(0.878890, value, places=6)

    def test_an_empty_bin_does_not_divide_by_zero(self):
        value = psi(np.array([1.0, 0.0]), np.array([0.5, 0.5]))

        self.assertTrue(np.isfinite(value))
        self.assertGreater(value, 0.0)

    def test_status_bands(self):
        for value, expected in ((0.0999, "stable"), (0.10, "warning"), (0.2499, "warning"), (0.25, "drift")):
            with self.subTest(value=value):
                self.assertEqual(expected, status(value))


if __name__ == "__main__":
    unittest.main()
