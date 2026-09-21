"""Post one date window of real rows to the drift detection API, then check its
answer against the same sums worked out offline.

Both sides call the same compare(), so a difference means the window in Redis
does not hold what was sent.

    cd api && PYTHONPATH=src:../ml/src uv run --group reference \\
        python tools/replay.py --start 2026-06-15 --end 2026-07-01
"""

import argparse
import sys
from pathlib import Path

import httpx
import numpy as np

from fraudstream_api.drift_detection.psi import bin_counts
from fraudstream_api.drift_detection.reference import Reference, compare
from fraudstream_api.drift_detection.snapshot import prepared_rows

TOLERANCE = 0.001


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--reference", default="reference/fraud-detection-v2.json")
    parser.add_argument("--url", default="http://localhost")
    parser.add_argument("--host", default="drift-detection.localhost")
    parser.add_argument("--batch", type=int, default=1000)
    return parser.parse_args()


def main() -> int:
    args = arguments()
    reference = Reference.from_json(Path(args.reference).read_text())
    frame, names = prepared_rows(reference.data_snapshot_id, args.start, args.end)
    values = frame[names].to_numpy()

    with httpx.Client(base_url=args.url, headers={"Host": args.host}, timeout=60) as http:
        for first in range(0, len(values), args.batch):
            observations = [
                {name: None if np.isnan(value) else float(value) for name, value in zip(names, row)}
                for row in values[first : first + args.batch]
            ]
            http.post("/v1/observations", json={"observations": observations}).raise_for_status()
        report = http.get("/v1/drift").json()

    counts = {
        name: bin_counts(frame[name].to_numpy(), reference.features[name].edges) for name in names
    }
    offline = {one.name: one for one in compare(reference, counts, len(frame))}

    print(
        f"  rows sent {len(frame):,}   counted by the API {report['observations']:,}"
        f"   status {report['status']}"
    )
    for feature in report["features"][:8]:
        expected = offline[feature["name"]]
        print(
            f"    {feature['name']:38s} API {feature['psi']:6.3f} {feature['status']:8s}"
            f" offline {expected.psi:6.3f}"
        )

    agree = report["observations"] == len(frame) and all(
        abs(feature["psi"] - offline[feature["name"]].psi) <= TOLERANCE
        for feature in report["features"]
    )
    print("  API and offline agree" if agree else "  API and offline DISAGREE")
    return 0 if agree else 1


if __name__ == "__main__":
    sys.exit(main())
