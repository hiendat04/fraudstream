"""Read one data version's rows through Trino and prepare them the way training did."""

from datetime import datetime

TRAINING_TABLE = "iceberg.ml.training_data"


def prepared_rows(
    snapshot_id: str, start: str, end: str, *, host: str = "localhost", port: int = 18082
):
    """Return the model's inputs for every row in [start, end) of one data version."""

    import pandas as pd
    import trino

    from fraudstream_ml.features import prepare_features

    # Parsing first means only real dates and a real number reach the SQL text.
    first = datetime.fromisoformat(start).strftime("%Y-%m-%d %H:%M:%S")
    last = datetime.fromisoformat(end).strftime("%Y-%m-%d %H:%M:%S")
    query = (
        f"SELECT * FROM {TRAINING_TABLE} FOR VERSION AS OF {int(snapshot_id)} "
        f"WHERE event_timestamp >= TIMESTAMP '{first}' "
        f"AND event_timestamp < TIMESTAMP '{last}'"
    )

    connection = trino.dbapi.connect(host=host, port=port, user="fraudstream", catalog="iceberg")
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
    finally:
        connection.close()

    frame = pd.DataFrame(rows, columns=columns)
    frame["event_timestamp"] = pd.to_datetime(frame["event_timestamp"], utc=True)
    prepared, names = prepare_features(frame)
    return prepared[names].astype(float), names
