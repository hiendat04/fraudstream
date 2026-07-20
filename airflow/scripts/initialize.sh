#!/usr/bin/env bash
set -euo pipefail

airflow db migrate
python /opt/airflow/config/bootstrap.py
airflow variables import \
    --action-on-existing-key overwrite \
    /opt/airflow/config/variables.json
airflow connections import \
    --overwrite \
    /tmp/fraudstream-airflow-bootstrap/connections.json
airflow pools import /tmp/fraudstream-airflow-bootstrap/pools.json

echo "Loaded FraudStream Airflow Variables, Connections, and Spark pool."
