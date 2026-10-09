"""Tests for the client that asks the KServe model for a score."""

import unittest

import httpx

from fraudstream_api.inference.model import (
    UNKNOWN_VERSION,
    ModelAnswer,
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

        self.assertEqual(0.42, score.probability)
        self.assertEqual(URL, sent["url"])
        self.assertIn(b'"instances"', sent["body"])

    async def test_it_returns_the_answering_version_and_threshold(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200, json={"predictions": [0.42], "model_version": "4", "threshold": 0.71}
            )

        model = client_answering(handler)
        try:
            answer = await model.score(INPUTS)
        finally:
            await model.close()

        self.assertEqual(ModelAnswer(0.42, "4", 0.71), answer)

    async def test_an_answer_without_a_version_is_unknown(self):
        """An older model server sends no version. Its answers still count, as unknown."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"predictions": [0.42]})

        model = client_answering(handler)
        try:
            answer = await model.score(INPUTS)
        finally:
            await model.close()

        self.assertEqual(ModelAnswer(0.42, UNKNOWN_VERSION, None), answer)

    async def test_it_waits_no_longer_than_its_timeout(self):
        model = ModelClient(URL, timeout_seconds=7.5)
        try:
            self.assertEqual(7.5, model._http.timeout.read)
        finally:
            await model.close()

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
