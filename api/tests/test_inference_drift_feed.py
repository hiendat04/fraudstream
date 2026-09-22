"""Tests for the feed that sends scored rows to the drift detection API.

Nothing here may raise: a prediction must never fail because the drift API is
having a bad day.
"""

import logging
import unittest

import httpx

from fraudstream_api.inference.drift_feed import DriftFeed

URL = "http://drift-detection/v1/observations"
INPUTS = {"amount": 36.35, "event_hour": 20.0, "customer_features_available": True}


def feed_answering(handler) -> DriftFeed:
    return DriftFeed(URL, timeout_seconds=2, transport=httpx.MockTransport(handler))


class DriftFeedTest(unittest.IsolatedAsyncioTestCase):
    async def test_it_posts_one_observation(self):
        sent = {}

        def handler(request: httpx.Request) -> httpx.Response:
            sent["url"] = str(request.url)
            sent["body"] = request.read()
            return httpx.Response(200, json={"accepted": 1})

        feed = feed_answering(handler)
        try:
            await feed.send(INPUTS)
        finally:
            await feed.close()

        self.assertEqual(URL, sent["url"])
        self.assertIn(b'"observations"', sent["body"])
        self.assertIn(b"36.35", sent["body"])

    async def test_a_down_drift_detector_is_logged_not_raised(self):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("no route", request=request)

        feed = feed_answering(handler)
        try:
            with self.assertLogs("fraudstream_api.inference.drift_feed", logging.WARNING) as logs:
                await feed.send(INPUTS)
        finally:
            await feed.close()

        self.assertIn("ConnectError", logs.output[0])
        self.assertIn("no route", logs.output[0])

    async def test_a_good_answer_is_not_logged(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"accepted": 1})

        feed = feed_answering(handler)
        try:
            with self.assertNoLogs("fraudstream_api.inference.drift_feed", level=logging.WARNING):
                await feed.send(INPUTS)
        finally:
            await feed.close()

    async def test_an_error_answer_is_logged_not_raised(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(422, json={"missing": ["amount"]})

        feed = feed_answering(handler)
        try:
            with self.assertLogs("fraudstream_api.inference.drift_feed", logging.WARNING) as logs:
                await feed.send(INPUTS)
        finally:
            await feed.close()

        self.assertIn("422", logs.output[0])
        self.assertIn("amount", logs.output[0])


if __name__ == "__main__":
    unittest.main()
