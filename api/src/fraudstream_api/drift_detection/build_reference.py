"""Build the drift reference for one model version from the rows it was trained on.

Run on the host, where Trino is reachable:

    cd api && PYTHONPATH=src:../ml/src uv run --group reference \\
        python -m fraudstream_api.drift_detection.build_reference \\
        --snapshot 3023861480409485916 --start 2026-01-01 --end 2026-05-05 \\
        --model-version 2 --out reference/fraud-detection-v2.json
"""

import argparse
from pathlib import Path

from fraudstream_api.drift_detection.reference import build_reference
from fraudstream_api.drift_detection.snapshot import prepared_rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--snapshot", required=True, help="the Iceberg snapshot the model trained on")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True, help="the pipeline's train_end: training rows stop here")
    parser.add_argument("--model-name", default="fraud-detection")
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--out", required=True)
    arguments = parser.parse_args()

    frame, names = prepared_rows(arguments.snapshot, arguments.start, arguments.end)
    reference = build_reference(
        {name: frame[name].to_numpy() for name in names},
        model_name=arguments.model_name,
        model_version=arguments.model_version,
        data_snapshot_id=arguments.snapshot,
    )
    Path(arguments.out).write_text(reference.to_json() + "\n")
    print(f"{reference.rows:,} rows, {len(reference.features)} features -> {arguments.out}")


if __name__ == "__main__":
    main()
