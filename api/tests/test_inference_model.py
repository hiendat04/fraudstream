"""Tests for the client that asks the KServe model for a score."""

import unittest

import httpx

from fraudstream_api.inference.model import (
    ModelClient,
    ModelRejected,
    ModelTimeout,
    ModelUnavailable,
)

URL = "http://model.example/v1/models/fraud-detection:predict"
INPUTS = {"amount": 36.35, "event_hour": 20.0, "customer_features_available": True}


def client_answering(handler) -> ModelClient:
    return ModelClient(URL, timeout_seconds=5, transport=httpx.MockTransport(handler))


class ModelClientTest(unittest.IsolatedAsyncioTestCase):
    async def test_it_sends_one_named_row_and_returns_the_probability(self):
        sent = {}

        def handler(request: httpx.Request) -> httpx.Response:
            sent["url"] = str(request.url)
            sent["body"] = request.read()
            return httpx.Response(200, json={"predictions": [0.42]})

        model = client_answering(handler)
        try:
            score = await model.score(INPUTS)
        finally:
            await model.close()

        self.assertEqual(0.42, score)
        self.assertEqual(URL, sent["url"])
        self.assertIn(b'"instances"', sent["body"])

    async def test_a_slow_model_becomes_model_timeout(self):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("too slow", request=request)

        model = client_answering(handler)
        try:
            with self.assertRaises(ModelTimeout):
                await model.score(INPUTS)
        finally:
            await model.close()

    async def test_an_unreachable_model_becomes_model_unavailable(self):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("no route", request=request)

        model = client_answering(handler)
        try:
            with self.assertRaises(ModelUnavailable):
                await model.score(INPUTS)
        finally:
            await model.close()

    async def test_an_error_answer_becomes_model_rejected_with_the_reason(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(400, json={"error": "these fields are missing: amount"})

        model = client_answering(handler)
        try:
            with self.assertRaises(ModelRejected) as caught:
                await model.score(INPUTS)
        finally:
            await model.close()

        self.assertIn("400", str(caught.exception))
        self.assertIn("amount", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
