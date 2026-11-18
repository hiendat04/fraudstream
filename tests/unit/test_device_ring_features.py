"""The device ring features, computed by Spark from payments."""

from __future__ import annotations

import importlib.util
from datetime import UTC, date, datetime, timedelta
from unittest import TestCase, main, skipUnless

DAY0 = date(2026, 6, 1)
PROCESSED_AT = datetime(2026, 7, 1, tzinfo=UTC)


def payment(customer_id, device_id="dev_ring_0001", ip_address=None, day=0, fraud=False):
    return (device_id, ip_address, customer_id, DAY0 + timedelta(days=day), fraud)


@skipUnless(importlib.util.find_spec("pyspark"), "PySpark is not installed")
class DeviceRingFeaturesTest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[1]").appName("DeviceRingFeaturesTest")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.addClassCleanup(cls.spark.stop)

    def features(self, *payments):
        from fraudstream.jobs.gold.transactions import _build_feat_device_ring

        frame = self.spark.createDataFrame(
            list(payments), "device_id string, ip_address string, customer_id string, event_date date, is_fraud boolean"
        )
        return {
            (row.device_id, row.feature_date): row
            for row in _build_feat_device_ring(frame, PROCESSED_AT).collect()
        }

    def test_one_customer_paying_every_day_counts_once(self) -> None:
        rows = self.features(*(payment("c1", day=day) for day in range(40)))

        self.assertEqual({row.device_customer_count_30d for row in rows.values()}, {1})

    def test_three_customers_on_one_device_with_one_fraud(self) -> None:
        rows = self.features(payment("c1"), payment("c2"), payment("c2", fraud=True), payment("c3"))

        row = rows[("dev_ring_0001", DAY0)]
        self.assertEqual((row.device_customer_count_30d, row.device_fraud_customer_count_30d), (3, 1))

    def test_a_customer_leaves_the_window_after_thirty_days(self) -> None:
        rows = self.features(payment("c1", day=0), payment("c2", day=29), payment("c2", day=30))

        self.assertEqual(rows[("dev_ring_0001", DAY0 + timedelta(days=29))].device_customer_count_30d, 2)
        self.assertEqual(rows[("dev_ring_0001", DAY0 + timedelta(days=30))].device_customer_count_30d, 1)

    def test_a_fraud_customer_leaves_the_window_too(self) -> None:
        rows = self.features(payment("c1", day=0, fraud=True), payment("c1", day=30))

        self.assertEqual(rows[("dev_ring_0001", DAY0)].device_fraud_customer_count_30d, 1)
        self.assertEqual(rows[("dev_ring_0001", DAY0 + timedelta(days=30))].device_fraud_customer_count_30d, 0)

    def test_a_crowded_ip_reaches_the_device(self) -> None:
        rows = self.features(payment("c1", "dev_a", "10.0.0.1"), payment("c2", "dev_b", "10.0.0.1"))

        row = rows[("dev_a", DAY0)]
        self.assertEqual((row.device_customer_count_30d, row.device_ip_customer_count_30d), (1, 2))

    def test_a_device_without_an_ip_has_zero_ip_customers(self) -> None:
        rows = self.features(payment("c1"))

        self.assertEqual(rows[("dev_ring_0001", DAY0)].device_ip_customer_count_30d, 0)

    def test_payments_without_a_device_write_no_rows(self) -> None:
        self.assertEqual(self.features(payment("c1", device_id=None, ip_address="10.0.0.1")), {})

    def test_a_snapshot_is_stamped_at_the_day_s_midnight(self) -> None:
        row = self.features(payment("c1"))[("dev_ring_0001", DAY0)]

        # collect() hands timestamps back in the machine's local zone, naive.
        self.assertEqual(row.event_timestamp.astimezone(UTC), datetime(2026, 6, 1, tzinfo=UTC))
        self.assertEqual(row.created.astimezone(UTC), PROCESSED_AT)


if __name__ == "__main__":
    main()
