"""Tests for the Trino tools that build the drift reference, with Trino mocked.

trino.dbapi.connect is replaced, so no query engine is needed. The rows it
returns are two real training rows, so prepare_features runs for real.
"""

import json
from unittest.mock import MagicMock

import pytest

from fraudstream_api.drift_detection import build_reference
from fraudstream_api.drift_detection.snapshot import prepared_rows

from tests.test_inference_features import NO_CUSTOMER_HISTORY, WITH_HISTORY

SNAPSHOT = "3023861480409485916"
ROWS = [WITH_HISTORY, NO_CUSTOMER_HISTORY]


@pytest.fixture
def trino(monkeypatch):
    """A mocked trino.dbapi.connect whose cursor returns the two rows."""

    columns = list(WITH_HISTORY)
    cursor = MagicMock()
    cursor.description = [(name, "varchar") for name in columns]
    cursor.fetchall.return_value = [[row[name] for name in columns] for row in ROWS]
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connect = MagicMock(return_value=connection)
    monkeypatch.setattr("trino.dbapi.connect", connect)
    return connect


def sent_query(trino) -> str:
    return trino.return_value.cursor.return_value.execute.call_args.args[0]


def test_the_query_pins_the_data_version_and_the_window(trino):
    prepared_rows(SNAPSHOT, "2026-06-15", "2026-07-01")

    query = sent_query(trino)
    assert f"FOR VERSION AS OF {SNAPSHOT}" in query
    assert "event_timestamp >= TIMESTAMP '2026-06-15 00:00:00'" in query
    assert "event_timestamp < TIMESTAMP '2026-07-01 00:00:00'" in query


def test_it_connects_to_the_iceberg_catalog_on_the_host(trino):
    prepared_rows(SNAPSHOT, "2026-06-15", "2026-07-01")

    trino.assert_called_once_with(
        host="localhost", port=18082, user="fraudstream", catalog="iceberg"
    )


def test_the_trino_address_can_be_changed(trino):
    prepared_rows(SNAPSHOT, "2026-06-15", "2026-07-01", host="trino", port=8080)

    assert trino.call_args.kwargs["host"] == "trino"
    assert trino.call_args.kwargs["port"] == 8080


def test_a_snapshot_id_that_is_not_a_number_is_refused(trino):
    with pytest.raises(ValueError):
        prepared_rows("1 OR 1=1", "2026-06-15", "2026-07-01")

    trino.assert_not_called()


def test_a_date_that_is_not_a_date_is_refused(trino):
    with pytest.raises(ValueError):
        prepared_rows(SNAPSHOT, "2026-06-15'; DROP TABLE x; --", "2026-07-01")

    trino.assert_not_called()


def test_the_connection_is_closed_even_when_the_query_fails(trino):
    trino.return_value.cursor.return_value.execute.side_effect = RuntimeError("Trino is down")

    with pytest.raises(RuntimeError):
        prepared_rows(SNAPSHOT, "2026-06-15", "2026-07-01")

    trino.return_value.close.assert_called_once()


def test_it_returns_the_51_prepared_inputs_as_floats(trino):
    frame, names = prepared_rows(SNAPSHOT, "2026-06-15", "2026-07-01")

    assert len(names) == 51
    assert list(frame.columns) == names
    assert len(frame) == 2
    assert all(str(dtype) == "float64" for dtype in frame.dtypes)
    assert frame.loc[0, "customer_features_available"] == 1.0
    assert frame.loc[1, "customer_features_available"] == 0.0


def test_build_reference_writes_the_file_and_reports_it(trino, tmp_path, monkeypatch, capsys):
    out = tmp_path / "reference.json"
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_reference",
            "--snapshot", SNAPSHOT,
            "--start", "2026-01-01",
            "--end", "2026-05-05",
            "--model-version", "2",
            "--out", str(out),
        ],
    )

    build_reference.main()

    written = json.loads(out.read_text())
    assert written["model_name"] == "fraud-detection"
    assert written["model_version"] == "2"
    assert written["data_snapshot_id"] == SNAPSHOT
    assert written["rows"] == 2
    assert len(written["features"]) == 51
    assert capsys.readouterr().out == f"2 rows, 51 features -> {out}\n"
