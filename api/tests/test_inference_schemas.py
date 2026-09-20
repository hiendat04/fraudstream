"""Tests for the payment the pull API accepts."""

import unittest
from datetime import UTC, datetime, timedelta

from pydantic import ValidationError

from fraudstream_api.inference.schemas import Transaction

VALID = {
    "transaction_id": "demo-000001",
    "customer_id": "cust_00103290",
    "merchant_id": "merch_00023570",
    "event_timestamp": "2026-06-30T20:00:00Z",
    "amount": 36.35,
    "channel": "atm",
    "city": "New York",
}


def payment(**changes):
    return Transaction(**{**VALID, **changes})


class TransactionTest(unittest.TestCase):
    def test_a_valid_payment_is_accepted(self):
        transaction = payment()

        self.assertEqual("cust_00103290", transaction.customer_id)
        self.assertEqual(36.35, transaction.amount)

    def test_an_unknown_channel_is_refused_and_the_allowed_ones_are_listed(self):
        with self.assertRaises(ValidationError) as caught:
            payment(channel="fax")

        self.assertIn("atm, card_present, mobile_wallet, online", str(caught.exception))

    def test_an_unknown_city_is_refused(self):
        """The model never saw it. All 12 city inputs would be 0 and it would still score."""

        with self.assertRaises(ValidationError):
            payment(city="Paris")

    def test_a_time_without_a_timezone_is_refused(self):
        """Without a timezone the hour input could shift without anyone noticing."""

        with self.assertRaises(ValidationError):
            payment(event_timestamp="2026-06-30T20:00:00")

    def test_a_time_is_converted_to_utc(self):
        transaction = payment(event_timestamp="2026-06-30T23:00:00+07:00")

        self.assertEqual(datetime(2026, 6, 30, 16, 0, tzinfo=UTC), transaction.event_timestamp)
        self.assertEqual(timedelta(0), transaction.event_timestamp.utcoffset())

    def test_a_time_in_the_future_is_refused(self):
        later = (datetime.now(UTC) + timedelta(minutes=10)).isoformat()

        with self.assertRaises(ValidationError):
            payment(event_timestamp=later)

    def test_a_zero_or_negative_amount_is_refused(self):
        for amount in (0, -5.0):
            with self.subTest(amount=amount), self.assertRaises(ValidationError):
                payment(amount=amount)

    def test_a_misspelled_field_is_refused(self):
        """A typo must be an error. Ignoring it would score the payment without it."""

        body = {**VALID, "ammount": 36.35}
        with self.assertRaises(ValidationError):
            Transaction(**body)


if __name__ == "__main__":
    unittest.main()
