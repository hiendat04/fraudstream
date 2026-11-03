"""Command-line entry point for local FraudStream utilities.

Run from the repo root (takes the offline generator's flags):

    PYTHONPATH=src uv run --extra storage python main.py --workers 6
"""

from fraudstream.generators.offline_transactions import main as generate_offline_transactions


def main() -> int:
    """Run the default offline transaction generator."""
    return generate_offline_transactions()


if __name__ == "__main__":
    raise SystemExit(main())
