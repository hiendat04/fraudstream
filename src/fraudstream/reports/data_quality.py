"""Generate a readable HTML report of generated data characteristics.

The report reads JSON evidence already produced by the offline generator,
streaming generator, and Silver transaction job. It does not scan transaction
contents or start Spark, so it remains fast and dependency-free.
"""

from __future__ import annotations

import argparse
import html
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


DEFAULT_OFFLINE_DIR = Path("data/raw_source/offline_transactions")
DEFAULT_SILVER_DIR = Path("data/silver/transactions")
DEFAULT_STREAMING_DIR = Path("data/raw_stream/transactions")
DEFAULT_OFFLINE_CONFIG = Path("configs/generator/offline_transactions.json")
DEFAULT_STREAMING_CONFIG = Path("configs/generator/streaming_transactions.json")
DEFAULT_REPORT_PATH = Path("reports/data_quality_report.html")


def _load_json(path: Path, *, required: bool = True) -> dict[str, Any] | None:
    """Load a JSON object, optionally returning ``None`` when it is absent."""

    if not path.exists():
        if required:
            raise FileNotFoundError(f"Required evidence file not found: {path}")
        return None
    with path.open(encoding="utf-8") as file:
        value = json.load(file)
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return value


def _directory_size(path: Path) -> int:
    """Return the total bytes stored under a directory without following links."""

    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _data_file_count(path: Path, suffix: str) -> int:
    """Count stored data files with the requested suffix."""

    return sum(1 for item in path.rglob(f"*{suffix}") if item.is_file())


def _number(value: int | float | None) -> str:
    """Format a numeric metric for terminal display."""

    if value is None:
        return "n/a"
    if isinstance(value, float) and not value.is_integer():
        return f"{value:,.2f}"
    return f"{int(value):,}"


def _percent_from_rate(value: float | int | None) -> str:
    """Format a zero-to-one rate as a percentage."""

    return "n/a" if value is None else f"{float(value) * 100:.2f}%"


def _bytes(value: int) -> str:
    """Format bytes with a compact binary unit."""

    amount = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if amount < 1024 or unit == "TiB":
            return f"{amount:.1f} {unit}"
        amount /= 1024
    return f"{amount:.1f} TiB"


def _html_value(value: object) -> str:
    """Escape one value before inserting it into the standalone report."""

    return html.escape(str(value))


def _html_table(rows: Sequence[tuple[str, object]]) -> str:
    """Render a compact two-column HTML table."""

    body = "".join(
        f"<tr><th scope='row'>{_html_value(label)}</th><td>{_html_value(value)}</td></tr>"
        for label, value in rows
    )
    return f"<table><tbody>{body}</tbody></table>"


def _html_distribution(distribution: Mapping[str, float], top_n: int) -> str:
    """Render leading distribution values as labeled percentage bars."""

    rows: list[str] = []
    for name, raw_percentage in list(distribution.items())[:top_n]:
        percentage = max(0.0, min(float(raw_percentage), 100.0))
        label = "<blank>" if not name.strip() else name
        rows.append(
            "<div class='bar-row'>"
            f"<span>{_html_value(label)}</span>"
            "<div class='bar-track' aria-hidden='true'>"
            f"<div class='bar-fill' style='width:{percentage:.2f}%'></div>"
            "</div>"
            f"<strong>{percentage:.2f}%</strong>"
            "</div>"
        )
    return "".join(rows)


def _html_config(config: Mapping[str, Any], keys: Sequence[str]) -> str:
    """Render selected generator configuration values."""

    return _html_table([(key, config.get(key, "n/a")) for key in keys])


def _offline_html(
    offline_dir: Path,
    silver_dir: Path,
    config_path: Path,
    top_n: int,
) -> str:
    """Build the offline section of the standalone HTML report."""

    summary_path = offline_dir / "_quality_summary.json"
    summary = _load_json(summary_path)
    config = _load_json(config_path)
    silver = _load_json(silver_dir / "_silver_transactions_summary.json", required=False)
    assert summary is not None and config is not None

    skew = summary["skew"]
    evolution = summary["schema_evolution"]
    old_rows = int(evolution["old_partition_row_count"])
    duplicate_after = (
        "0 retained"
        if silver is not None
        else "Not available — run Silver first"
    )
    silver_rows = _number(silver["output_row_count"]) if silver is not None else "n/a"
    removed_rows = _number(silver["duplicate_rows_removed_count"]) if silver is not None else "n/a"

    storage_rows: list[tuple[str, object]] = [
        ("Raw format", "Partitioned CSV"),
        ("Raw rows", _number(summary["row_count_after_duplicates"])),
        ("CSV partitions", _number(summary["written_file_count"])),
        ("Raw size on disk", _bytes(_directory_size(offline_dir))),
    ]
    if silver is not None:
        storage_rows.extend(
            [
                ("Silver format", "Parquet"),
                ("Silver rows", silver_rows),
                ("Silver Parquet files", _number(_data_file_count(silver_dir, ".parquet"))),
                ("Silver size on disk", _bytes(_directory_size(silver_dir))),
            ]
        )

    cardinality_rows = [
        (key.removeprefix("approx_count_distinct_"), _number(value))
        for key, value in summary["high_cardinality"].items()
    ]
    config_keys = (
        "random_seed",
        "n_transactions",
        "n_customers",
        "n_accounts",
        "n_merchants",
        "skew_city_ratio",
        "skew_merchant_category_ratio",
        "duplicate_rate",
        "late_arrival_rate",
        "schema_change_date",
    )

    return f"""
    <section>
      <div class="section-heading">
        <div><span class="eyebrow">Offline source → Silver</span><h2>Offline data characteristics</h2></div>
        <span class="badge measured">Measured</span>
      </div>
      <div class="metric-grid">
        <article class="metric"><span>Raw rows</span><strong>{_number(summary['row_count_after_duplicates'])}</strong><small>Partitioned CSV</small></article>
        <article class="metric"><span>Dominant city</span><strong>{next(iter(skew['city_distribution_pct'].values())):.2f}%</strong><small>{_html_value(next(iter(skew['city_distribution_pct'].keys())))}</small></article>
        <article class="metric"><span>Raw duplicates</span><strong>{_percent_from_rate(summary['duplicate_rate_actual'])}</strong><small>{_number(summary['duplicate_row_count'])} rows</small></article>
        <article class="metric"><span>After dedup</span><strong>{_html_value(duplicate_after)}</strong><small>{silver_rows} Silver rows</small></article>
      </div>
      <div class="two-column">
        <article><h3>Storage and volume</h3>{_html_table(storage_rows)}</article>
        <article><h3>Duplicate handling</h3>{_html_table([
            ('Before Silver', f"{_number(summary['duplicate_row_count'])} / {_number(summary['row_count_after_duplicates'])} ({_percent_from_rate(summary['duplicate_rate_actual'])})"),
            ('Removed by Silver', removed_rows),
            ('Retained duplicates', duplicate_after),
        ])}</article>
      </div>
      <div class="two-column">
        <article><h3>City skew</h3>{_html_distribution(skew['city_distribution_pct'], top_n)}</article>
        <article><h3>Merchant-category skew</h3>{_html_distribution(skew['merchant_category_distribution_pct'], top_n)}</article>
      </div>
      <div class="two-column">
        <article><h3>ID cardinality</h3><p class="hint">Generator evidence fields named approx_count_distinct.</p>{_html_table(cardinality_rows)}</article>
        <article><h3>Schema evolution</h3>{_html_table([
            ('Change date', evolution['schema_change_date']),
            ('Old v1 rows', _number(old_rows)),
            ('New v2 rows', _number(evolution['new_partition_row_count'])),
            ('Columns absent from v1', ', '.join(evolution['old_partition_missing_columns'])),
            ('Nulls after unification', f"{_number(old_rows)} per evolved column"),
        ])}</article>
      </div>
      <div class="two-column">
        <article><h3>Other source problems</h3>{_html_table([
            ('Late-arrival rate', _percent_from_rate(summary['late_arrivals']['late_arrival_rate_actual'])),
            ('Missing-value row rate', _percent_from_rate(summary['raw_quality_issues']['missing_value_rate_actual'])),
            ('Inconsistent-format rate', _percent_from_rate(summary['raw_quality_issues']['inconsistent_format_rate_actual'])),
        ])}</article>
        <article><h3>Generator configuration</h3>{_html_config(config, config_keys)}</article>
      </div>
      <p class="source">Evidence: <code>{_html_value(summary_path)}</code> · Config: <code>{_html_value(config_path)}</code></p>
    </section>"""


def _streaming_html(streaming_dir: Path, config_path: Path) -> str:
    """Build the streaming section using measurements or labeled targets."""

    summary_path = streaming_dir / "_stream_summary.json"
    summary = _load_json(summary_path, required=False)
    config = _load_json(config_path)
    assert config is not None
    config_keys = (
        "random_seed",
        "n_events",
        "n_customers",
        "n_merchants",
        "topic",
        "n_partitions",
        "window_minutes",
        "burst_window_count",
        "burst_event_ratio",
        "late_event_rate",
        "out_of_order_rate",
        "duplicate_rate",
    )

    if summary is None:
        metrics = [
            ("Burst events", config["burst_event_ratio"]),
            ("Late events", config["late_event_rate"]),
            ("Duplicate records", config["duplicate_rate"]),
            ("Out-of-order events", config["out_of_order_rate"]),
        ]
        problem_rows = "".join(
            "<div class='rate-row'>"
            f"<span>{_html_value(label)}</span>"
            f"<strong>{_percent_from_rate(rate)}</strong>"
            "</div>"
            for label, rate in metrics
        )
        return f"""
        <section>
          <div class="section-heading">
            <div><span class="eyebrow">Streaming source</span><h2>Streaming data characteristics</h2></div>
            <span class="badge configured">Configured targets</span>
          </div>
          <div class="notice">Measured stream evidence is not available. Generate the streaming dataset, then rebuild this report.</div>
          <div class="two-column">
            <article><h3>Configured problem rates</h3>{problem_rows}</article>
            <article><h3>Generator configuration</h3>{_html_config(config, config_keys)}</article>
          </div>
          <p class="source">Expected evidence: <code>{_html_value(summary_path)}</code> · Config: <code>{_html_value(config_path)}</code></p>
        </section>"""

    problems = summary["stream_problems"]
    windows = summary["event_time_windows"]
    measured_rates = [
        ("Burst events", problems["burst_event_rate_actual"], problems["burst_event_count"]),
        ("Late events", problems["late_event_rate_actual"], problems["late_event_count"]),
        ("Duplicate records", summary["duplicate_rate_actual"], summary["duplicate_record_count"]),
    ]
    problem_bars = "".join(
        "<div class='bar-row'>"
        f"<span>{_html_value(label)}</span>"
        "<div class='bar-track' aria-hidden='true'>"
        f"<div class='bar-fill stream' style='width:{max(0.0, min(float(rate) * 100, 100.0)):.2f}%'></div>"
        "</div>"
        f"<strong>{_percent_from_rate(rate)}</strong>"
        f"<small>{_number(count)}</small>"
        "</div>"
        for label, rate, count in measured_rates
    )

    return f"""
    <section>
      <div class="section-heading">
        <div><span class="eyebrow">Streaming source</span><h2>Streaming data characteristics</h2></div>
        <span class="badge measured">Measured</span>
      </div>
      <div class="metric-grid">
        <article class="metric"><span>Records</span><strong>{_number(summary['record_count_after_duplicates'])}</strong><small>{_number(summary['base_event_count'])} base events</small></article>
        <article class="metric"><span>Burst rate</span><strong>{_percent_from_rate(problems['burst_event_rate_actual'])}</strong><small>{_number(problems['burst_event_count'])} events</small></article>
        <article class="metric"><span>Late rate</span><strong>{_percent_from_rate(problems['late_event_rate_actual'])}</strong><small>{_number(problems['late_event_count'])} events</small></article>
        <article class="metric"><span>Duplicate rate</span><strong>{_percent_from_rate(summary['duplicate_rate_actual'])}</strong><small>{_number(summary['duplicate_record_count'])} records</small></article>
      </div>
      <div class="two-column">
        <article><h3>Storage and volume</h3>{_html_table([
            ('Format', f"{summary['sink_type']} — JSONL Kafka replay source"),
            ('Kafka topic', summary['topic']),
            ('Partitions', _number(summary['n_partitions'])),
            ('JSONL files', _number(_data_file_count(streaming_dir, '.jsonl'))),
            ('Size on disk', _bytes(_directory_size(streaming_dir))),
        ])}</article>
        <article><h3>Measured problem rates</h3>{problem_bars}</article>
      </div>
      <div class="two-column">
        <article><h3>Event-time behavior</h3>{_html_table([
            ('Window length', f"{windows['window_minutes']} minutes"),
            ('Distinct windows', _number(windows['window_count'])),
            ('Maximum records in one window', _number(windows['max_records_in_window'])),
            ('Observed out-of-order events', _number(problems['observed_out_of_order_event_count'])),
        ])}</article>
        <article><h3>Generator configuration</h3>{_html_config(config, config_keys)}</article>
      </div>
      <p class="source">Evidence: <code>{_html_value(summary_path)}</code> · Config: <code>{_html_value(config_path)}</code></p>
    </section>"""


def render_html_report(
    *,
    dataset: str,
    offline_dir: Path,
    silver_dir: Path,
    streaming_dir: Path,
    offline_config: Path,
    streaming_config: Path,
    top_n: int,
) -> str:
    """Render a self-contained, printable HTML quality report."""

    sections: list[str] = []
    if dataset in ("all", "offline"):
        sections.append(_offline_html(offline_dir, silver_dir, offline_config, top_n))
    if dataset in ("all", "streaming"):
        sections.append(_streaming_html(streaming_dir, streaming_config))
    generated_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>FraudStream Data Quality Report</title>
  <style>
    :root {{ color-scheme: light dark; --bg:#f5f7fb; --surface:#ffffff; --text:#172033; --muted:#667085; --line:#d9dfeb; --accent:#3157d5; --stream:#e1702c; --soft:#eef2ff; --good:#176b4d; }}
    @media (prefers-color-scheme: dark) {{ :root {{ --bg:#111521; --surface:#191f2e; --text:#eef2ff; --muted:#aab4c8; --line:#343d52; --soft:#222b43; --good:#68d4aa; }} }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; background:var(--bg); color:var(--text); font-family:Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; line-height:1.45; }}
    main {{ width:min(1180px,calc(100% - 32px)); margin:32px auto 64px; }}
    header {{ margin-bottom:28px; }}
    h1 {{ margin:4px 0 6px; font-size:clamp(28px,4vw,44px); letter-spacing:-.03em; }}
    h2 {{ margin:2px 0 0; font-size:26px; }}
    h3 {{ margin:0 0 14px; font-size:17px; }}
    p {{ margin:0; }}
    .eyebrow {{ color:var(--accent); font-size:12px; font-weight:700; letter-spacing:.09em; text-transform:uppercase; }}
    .subtitle,.hint,.source,small {{ color:var(--muted); }}
    .subtitle {{ max-width:760px; }}
    section {{ margin-top:24px; padding:24px; background:var(--surface); border:1px solid var(--line); border-radius:18px; }}
    .section-heading {{ display:flex; justify-content:space-between; align-items:flex-start; gap:16px; margin-bottom:20px; }}
    .badge {{ padding:5px 10px; border-radius:999px; font-size:12px; font-weight:700; white-space:nowrap; }}
    .measured {{ color:var(--good); background:color-mix(in srgb,var(--good) 12%,transparent); }}
    .configured {{ color:var(--stream); background:color-mix(in srgb,var(--stream) 12%,transparent); }}
    .metric-grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; margin-bottom:22px; }}
    .metric {{ padding:16px; background:var(--soft); border-radius:12px; }}
    .metric span,.metric small {{ display:block; }}
    .metric strong {{ display:block; margin:5px 0 2px; font-size:24px; letter-spacing:-.02em; }}
    .two-column {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:26px; margin-top:24px; }}
    table {{ width:100%; border-collapse:collapse; font-size:14px; }}
    th,td {{ padding:8px 0; border-bottom:1px solid var(--line); vertical-align:top; }}
    th {{ width:52%; color:var(--muted); font-weight:500; text-align:left; }}
    td {{ text-align:right; overflow-wrap:anywhere; }}
    .bar-row {{ display:grid; grid-template-columns:minmax(110px,1.2fr) minmax(90px,2fr) 64px; gap:10px; align-items:center; margin:10px 0; font-size:14px; }}
    .bar-row small {{ grid-column:3; text-align:right; }}
    .bar-track {{ height:10px; overflow:hidden; border-radius:999px; background:var(--line); }}
    .bar-fill {{ height:100%; border-radius:inherit; background:var(--accent); }}
    .bar-fill.stream {{ background:var(--stream); }}
    .bar-row strong {{ text-align:right; font-variant-numeric:tabular-nums; }}
    .rate-row {{ display:flex; justify-content:space-between; padding:9px 0; border-bottom:1px solid var(--line); }}
    .notice {{ padding:12px 14px; background:var(--soft); border-left:4px solid var(--stream); }}
    .hint {{ margin:-8px 0 8px; font-size:12px; }}
    .source {{ margin-top:22px; font-size:12px; overflow-wrap:anywhere; }}
    code {{ font-family:"SFMono-Regular",Consolas,monospace; }}
    footer {{ margin-top:20px; color:var(--muted); font-size:12px; text-align:right; }}
    @media (max-width:760px) {{ .metric-grid,.two-column {{ grid-template-columns:1fr 1fr; }} .metric-grid {{ grid-template-columns:1fr 1fr; }} }}
    @media (max-width:520px) {{ main {{ width:min(100% - 20px,1180px); margin-top:16px; }} section {{ padding:17px; }} .metric-grid,.two-column {{ grid-template-columns:1fr; }} .section-heading {{ align-items:flex-start; }} .bar-row {{ grid-template-columns:100px 1fr 58px; }} }}
    @media print {{ :root {{ color-scheme:light; }} body {{ background:#fff; }} main {{ width:100%; margin:0; }} section {{ break-inside:avoid; box-shadow:none; }} }}
  </style>
</head>
<body>
  <main>
    <header><span class="eyebrow">FraudStream evidence</span><h1>Generated Data Quality Report</h1><p class="subtitle">Measured offline and streaming characteristics from reproducible generator and Silver evidence artifacts.</p></header>
    {''.join(sections)}
    <footer>Generated {generated_at}</footer>
  </main>
</body>
</html>
"""


def build_parser() -> argparse.ArgumentParser:
    """Build command-line arguments for the evidence report."""

    parser = argparse.ArgumentParser(
        description="Generate a standalone HTML report from data-quality evidence.",
    )
    parser.add_argument("--dataset", choices=("all", "offline", "streaming"), default="all")
    parser.add_argument("--offline-dir", type=Path, default=DEFAULT_OFFLINE_DIR)
    parser.add_argument("--silver-dir", type=Path, default=DEFAULT_SILVER_DIR)
    parser.add_argument("--streaming-dir", type=Path, default=DEFAULT_STREAMING_DIR)
    parser.add_argument("--offline-config", type=Path, default=DEFAULT_OFFLINE_CONFIG)
    parser.add_argument("--streaming-config", type=Path, default=DEFAULT_STREAMING_CONFIG)
    parser.add_argument("--top-n", type=int, default=5, help="Number of skew values to display.")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_REPORT_PATH,
        help="Destination for the standalone HTML report.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Write the selected evidence to a standalone HTML report."""

    args = build_parser().parse_args(argv)
    if args.top_n < 1:
        raise ValueError("--top-n must be at least 1")

    report = render_html_report(
        dataset=args.dataset,
        offline_dir=args.offline_dir,
        silver_dir=args.silver_dir,
        streaming_dir=args.streaming_dir,
        offline_config=args.offline_config,
        streaming_config=args.streaming_config,
        top_n=args.top_n,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report, encoding="utf-8")
    print(f"Wrote data-quality report: {args.output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
