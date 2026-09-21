"""Binning and the population stability index (PSI)."""

import numpy as np

EPSILON = 1e-4
WARNING = 0.10
DRIFT = 0.25
# A feature with this many distinct values or fewer gets one bin per value.
FEW_VALUES = 10


def bin_edges(reference_values: np.ndarray) -> np.ndarray:
    """Choose the bin edges for one feature from its training values."""

    values = reference_values[~np.isnan(reference_values)]
    distinct = np.unique(values)
    if len(distinct) <= FEW_VALUES:
        # One bin per value. Deciles would merge a rare 1 into the bin of the common 0.
        return (distinct[:-1] + distinct[1:]) / 2
    return np.unique(np.quantile(values, np.linspace(0.1, 0.9, 9)))


def bin_counts(values: np.ndarray, edges: np.ndarray) -> np.ndarray:
    """Count values per bin. The last bin counts missing values."""

    missing = len(edges) + 1
    index = np.where(
        np.isnan(values), missing, np.searchsorted(edges, np.nan_to_num(values), side="right")
    )
    return np.bincount(index.astype(int), minlength=len(edges) + 2)


def psi(reference: np.ndarray, current: np.ndarray) -> float:
    """PSI between two sets of bin shares."""

    # An empty bin would divide by zero, so every share counts as at least EPSILON.
    expected = np.clip(reference, EPSILON, None)
    seen = np.clip(current, EPSILON, None)
    return float(np.sum((seen - expected) * np.log(seen / expected)))


def status(value: float) -> str:
    """The usual PSI bands."""

    if value >= DRIFT:
        return "drift"
    if value >= WARNING:
        return "warning"
    return "stable"
