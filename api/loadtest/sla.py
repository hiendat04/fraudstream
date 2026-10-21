"""The SLA check every load test runs when Locust stops: a missed limit exits 1."""


def check_sla(environment, sla: dict[str, float]) -> None:
    total = environment.stats.total
    measured = {
        "p95_ms": total.get_response_time_percentile(0.95),
        "p99_ms": total.get_response_time_percentile(0.99),
        "failure_ratio": total.fail_ratio,
    }
    missed = [name for name, limit in sla.items() if measured[name] > limit]
    print(f"\n  requests {total.num_requests}  failures {total.num_failures}"
          f"  rate {total.total_rps:.1f}/s  median {total.median_response_time} ms")
    for name, limit in sla.items():
        print(f"  SLA {name:14s} {measured[name]:>8.3f}  limit {limit:<6} "
              f"{'MISSED' if name in missed else 'met'}")
    if missed:
        environment.process_exit_code = 1
