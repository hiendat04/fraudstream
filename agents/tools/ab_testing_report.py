"""Reads one window of an A/B split from Prometheus and judges it: pass, fail or inconclusive.

    python tools/ab_testing_report.py llm --prometheus http://127.0.0.1:19090 [--window 30m]
    python tools/ab_testing_report.py agent --prometheus http://127.0.0.1:19090 --window 2h

Prints a row per variant, then the verdict and its reasons. Exits 0 on pass, 10 on fail
and 12 when the candidate has too few calls to say.
"""

import argparse
import sys
import time

import httpx

from fraudstream_agents.ab_testing import MIN_CALLS, Judgement, Variant, Verdict, judge, read_variants

EXIT_CODES = {Verdict.PASS: 0, Verdict.FAIL: 10, Verdict.INCONCLUSIVE: 12}


def row(variant: Variant, now: float) -> str:
    p95 = "-" if variant.p95_seconds is None else f"{variant.p95_seconds:.2f}"
    golden = "-" if variant.golden_asked is None else f"{variant.golden_passed}/{variant.golden_asked}"
    if variant.golden_pushed is not None:
        golden += f" ({(now - variant.golden_pushed) / 60:.0f} min ago)"
    return f"  {variant.name:<14} {variant.calls:>7.0f} {variant.failures:>8.0f} {p95:>8}   {golden}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=sorted(MIN_CALLS))
    parser.add_argument("--prometheus", required=True)
    parser.add_argument("--window", default="30m")
    args = parser.parse_args(argv)

    with httpx.Client(base_url=args.prometheus, timeout=30) as client:
        baseline, candidate, unnamed = read_variants(client, args.kind, args.window)
    judgement: Judgement = judge(baseline, candidate, min_calls=MIN_CALLS[args.kind], unnamed_failures=unnamed)

    now = time.time()
    print(f"==> {args.kind} split, last {args.window}")
    print(f"  {'variant':<14} {'calls':>7} {'failures':>8} {'p95 s':>8}   golden")
    print(row(baseline, now))
    print(row(candidate, now))
    if unnamed:
        print(f"  {unnamed:.0f} failures reached neither variant")
    print(f"  {judgement.verdict.value}")
    for reason in judgement.reasons:
        print(f"    - {reason}")
    return EXIT_CODES[judgement.verdict]


if __name__ == "__main__":
    sys.exit(main())
