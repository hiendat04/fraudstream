"""Contract tests for the chronological train/validation/test split."""

from __future__ import annotations

import unittest

import pandas as pd

from fraudstream_ml.split import SPLIT_NAMES, chronological_split


TRAIN_END = "2026-05-05"
VALIDATION_END = "2026-05-30"


def _frame(timestamps: list[str], labels: list[int] | None = None) -> pd.DataFrame:
    """Build a minimal labeled frame with one transaction per timestamp."""

    if labels is None:
        # Alternate so every split ends up with both classes present.
        labels = [index % 2 for index in range(len(timestamps))]
    return pd.DataFrame(
        {
            "transaction_id": [f"txn_{index:03d}" for index in range(len(timestamps))],
            "event_timestamp": pd.to_datetime(timestamps),
            "is_fraud": labels,
        }
    )


SPREAD = [
    "2026-01-15", "2026-02-15", "2026-03-15", "2026-04-15",
    "2026-05-10", "2026-05-12", "2026-05-20", "2026-05-25",
    "2026-06-05", "2026-06-10", "2026-06-20", "2026-06-25",
]


class ChronologicalSplitTest(unittest.TestCase):
    def test_splits_are_chronological_and_disjoint(self):
        """Training must end before validation starts, which must end before test starts.

        This is the property a random split destroys: rolling features encode
        a customer's history, so a future row placed in training lets the
        model learn from behaviour it could not have observed at scoring time.
        """

        splits = chronological_split(_frame(SPREAD), TRAIN_END, VALIDATION_END)

        train_max = splits["train"]["event_timestamp"].max()
        validation_min = splits["validation"]["event_timestamp"].min()
        validation_max = splits["validation"]["event_timestamp"].max()
        test_min = splits["test"]["event_timestamp"].min()

        self.assertLess(train_max, validation_min)
        self.assertLess(validation_max, test_min)

    def test_no_transaction_appears_in_two_splits(self):
        splits = chronological_split(_frame(SPREAD), TRAIN_END, VALIDATION_END)

        seen = [id_ for split in SPLIT_NAMES for id_ in splits[split]["transaction_id"]]

        self.assertEqual(len(seen), len(set(seen)))

    def test_every_row_lands_in_exactly_one_split(self):
        """A dropped row is silent data loss, so the three splits must sum to the input."""

        frame = _frame(SPREAD)

        splits = chronological_split(frame, TRAIN_END, VALIDATION_END)

        self.assertEqual(sum(len(splits[name]) for name in SPLIT_NAMES), len(frame))

    def test_boundaries_are_inclusive_start_exclusive_end(self):
        """A row exactly on a boundary belongs to the later split, and only that one."""

        frame = _frame(
            ["2026-01-01", "2026-05-05", "2026-05-30", "2026-06-01"],
            labels=[0, 1, 0, 1],
        )

        splits = chronological_split(frame, TRAIN_END, VALIDATION_END, require_both_classes=False)

        self.assertEqual(list(splits["train"]["transaction_id"]), ["txn_000"])
        self.assertEqual(list(splits["validation"]["transaction_id"]), ["txn_001"])
        self.assertEqual(list(splits["test"]["transaction_id"]), ["txn_002", "txn_003"])

    def test_accepts_timezone_aware_timestamps(self):
        """Feast returns UTC-aware timestamps, and comparing those to naive bounds raises."""

        frame = _frame(SPREAD)
        frame["event_timestamp"] = frame["event_timestamp"].dt.tz_localize("UTC")

        splits = chronological_split(frame, TRAIN_END, VALIDATION_END)

        self.assertEqual(sum(len(splits[name]) for name in SPLIT_NAMES), len(frame))
        self.assertTrue(len(splits["validation"]) > 0)

    def test_rejects_boundaries_out_of_order(self):
        with self.assertRaises(ValueError):
            chronological_split(_frame(SPREAD), VALIDATION_END, TRAIN_END)

    def test_rejects_an_empty_split(self):
        """An empty split fails later and more confusingly, inside model fitting."""

        frame = _frame(["2026-01-10", "2026-01-20"], labels=[0, 1])

        with self.assertRaises(ValueError):
            chronological_split(frame, TRAIN_END, VALIDATION_END)

    def test_rejects_a_split_with_only_one_class(self):
        """PR-AUC is undefined without positives; fail here rather than at scoring time."""

        frame = _frame(
            ["2026-01-10", "2026-02-10", "2026-05-10", "2026-05-20", "2026-06-10", "2026-06-20"],
            labels=[0, 0, 1, 0, 1, 0],
        )

        with self.assertRaises(ValueError):
            chronological_split(frame, TRAIN_END, VALIDATION_END)


if __name__ == "__main__":
    unittest.main()
