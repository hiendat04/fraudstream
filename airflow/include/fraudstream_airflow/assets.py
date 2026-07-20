"""Validated Airflow assets connecting FraudStream batch DAGs."""

from airflow.sdk import Asset


BRONZE_TRANSACTIONS_VALIDATED = Asset(
    "x-fraudstream://bronze/raw_transactions/validated"
)
CORE_GOLD_TRANSACTIONS_VALIDATED = Asset(
    "x-fraudstream://gold/core_transactions/validated"
)
OFFLINE_FEATURES_VALIDATED = Asset(
    "x-fraudstream://gold/offline_features/validated"
)
