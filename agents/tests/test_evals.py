"""The golden questions: reading the decision and the citations, and every way an answer can fall short."""

import json

import httpx
import pytest

from fraudstream_agents.a2a import Answer
from fraudstream_agents.evals import GOLDEN_QUESTIONS, PAYMENT, GoldenQuestion, cited_chunks, decision, problems, score_reported
from fraudstream_agents.prompts import PROMPTS_DIR


@pytest.mark.parametrize(("text", "flagged"), [
    pytest.param("- p 0.81\nModel decision: flagged", True, id="flagged"),
    pytest.param("Model decision: not flagged", False, id="not-flagged"),
    pytest.param("model decision:   NOT FLAGGED", False, id="any-case-and-spacing"),
    pytest.param("Model decision: flagged\nlater\nModel decision: not flagged", False, id="the-last-line-counts"),
    pytest.param("Model decision: not flagged\nlater\nModel decision: flagged", True, id="the-last-line-counts-either-way"),
    pytest.param("The payment was not flagged.", None, id="only-the-decision-line-counts"),
    pytest.param("", None, id="nothing"),
])
def test_the_model_decision_line(text, flagged):
    assert decision(text) is flagged


@pytest.mark.parametrize(("text", "chunks"), [
    pytest.param("See [handbook/review-policy#blocking-a-payment] and [handbook/review-policy#blocking-a-payment].", ["handbook/review-policy#blocking-a-payment"], id="each-once"),
    pytest.param("[model-cards/fraud-detection-v12#metrics] [feature-dictionary/merchant_risk_features#overview-2]",
                 ["model-cards/fraud-detection-v12#metrics", "feature-dictionary/merchant_risk_features#overview-2"], id="in-order"),
    pytest.param("handbook/review-policy#blocking-a-payment without brackets", [], id="brackets-needed"),
    pytest.param("[a note] [review-policy]", [], id="brackets-without-a-hash"),
])
def test_the_chunks_an_answer_cites(text, chunks):
    assert cited_chunks(text) == chunks


SCORED = {"flagged": True, "threshold": 0.37, "fraud_probability": 0.81}


@pytest.mark.parametrize(("text", "prediction", "reported"), [
    pytest.param("probability 0.81, threshold 0.37\nModel decision: flagged", SCORED, True, id="decision-and-threshold"),
    pytest.param("threshold 0.37\nModel decision: not flagged", SCORED, False, id="the-wrong-decision"),
    pytest.param("threshold 0.37", SCORED, False, id="no-decision-line"),
    pytest.param("Model decision: flagged", SCORED, False, id="no-threshold-quoted"),
    # 0.3737 is 1% off on paper, but not in floating point, so the pair sits just either side.
    pytest.param("threshold 0.3736\nModel decision: flagged", SCORED, True, id="just-under-1-percent-counts"),
    pytest.param("threshold 0.3738\nModel decision: flagged", SCORED, False, id="just-over-1-percent"),
    pytest.param("The model has no threshold.", {"flagged": None, "threshold": None}, True, id="no-threshold-no-line"),
    pytest.param("Model decision: flagged", {"flagged": None, "threshold": None}, False, id="a-line-without-a-threshold"),
])
def test_the_score_is_reported_as_the_model_gave_it(text, prediction, reported):
    assert score_reported(text, prediction) is reported


CHUNKS = {"handbook/review-policy#blocking-a-payment", "model-cards/m#a"}
EVERYTHING = GoldenQuestion("coordinator-agent", "q", frozenset({"score_payment", "search_handbook"}),
                    customer="cust_00090571", merchant="merch_00011503", payment=True, handbook="handbook/review-policy")
GOOD = "- 17 payments in a day\n- 4 declines\n- threshold 0.37 [handbook/review-policy#blocking-a-payment]\nModel decision: flagged"


def truth(request):
    path = request.url.path
    if path == "/v1/customers/cust_00090571":
        return httpx.Response(200, json={"views": {"v": {"features": {"txn_count_24h": 17}}}})
    if path == "/v1/merchants/merch_00011503":
        return httpx.Response(200, json={"views": {"v": {"features": {"declines_24h": 4, "high_risk": True}}}})
    if path == "/v1/predict":
        assert json.loads(request.content) == PAYMENT
        return httpx.Response(200, json={"flagged": True, "threshold": 0.37, "fraud_probability": 0.81})
    if path == "/v1/chunks":
        chunk_id = request.url.params["chunk_id"]
        return httpx.Response(200 if chunk_id in CHUNKS else 404, json={"chunk_id": chunk_id})
    return httpx.Response(500)


async def found(answer, golden=EVERYTHING, serve=truth):
    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as client:
        return await problems(client, golden, answer, "http://context", "http://inference")


def answer(text=GOOD, state="completed", tools=("search_handbook", "score_payment")):
    return Answer(state, text, list(tools))


@pytest.mark.anyio
async def test_a_good_answer_has_no_problems():
    assert await found(answer()) == []


@pytest.mark.anyio
async def test_an_unfinished_answer():
    assert await found(answer(state="failed")) == ["the answer ended failed"]


@pytest.mark.anyio
async def test_a_missing_tool_call():
    assert await found(answer(tools=["score_payment", "score_payment"])) == ["did not call search_handbook"]


@pytest.mark.anyio
async def test_every_missing_tool_call_in_name_order():
    assert await found(answer(tools=[])) == ["did not call score_payment", "did not call search_handbook"]


@pytest.mark.anyio
async def test_customer_values_not_quoted():
    assert await found(answer(GOOD.replace("17", "16"))) == ["quotes none of cust_00090571's stored values"]


@pytest.mark.anyio
async def test_merchant_values_not_quoted():
    assert await found(answer(GOOD.replace("4 declines", "no declines"))) == ["quotes none of merch_00011503's stored values"]


@pytest.mark.anyio
async def test_the_wrong_decision():
    assert await found(answer(GOOD.replace("flagged", "not flagged"))) == [
        "does not report the model's decision and threshold for txn_000000128913"]


@pytest.mark.anyio
async def test_no_citation():
    assert await found(answer(GOOD.replace("[handbook/review-policy#blocking-a-payment]", ""))) == ["cites no chunk"]


@pytest.mark.anyio
async def test_a_citation_that_does_not_exist():
    assert await found(answer(GOOD + " [handbook/x#y]")) == ["cites handbook/x#y, which does not exist"]


@pytest.mark.anyio
async def test_nothing_under_the_prefix():
    assert await found(answer(GOOD.replace("handbook/review-policy#blocking-a-payment", "model-cards/m#a"))) == [
        "cites nothing under handbook/review-policy"]


@pytest.mark.anyio
async def test_any_existing_chunk_will_do_without_a_prefix():
    golden = GoldenQuestion("risk-agent", "q", frozenset(), handbook="")
    assert await found(answer("[model-cards/m#a]"), golden) == []


@pytest.mark.anyio
async def test_only_what_the_question_asks_is_checked():
    assert await found(answer("nothing", tools=["get_chunk"]), GoldenQuestion("context-agent", "q", frozenset({"get_chunk"}))) == []


@pytest.mark.anyio
@pytest.mark.parametrize("broken", ["/v1/customers/cust_00090571", "/v1/predict", "/v1/chunks"])
async def test_a_truth_request_that_fails_raises(broken):
    def serve(request):
        return httpx.Response(503) if request.url.path == broken else truth(request)

    with pytest.raises(httpx.HTTPStatusError):
        await found(answer(), serve=serve)


def test_the_golden_questions_are_for_the_three_agents_and_each_has_some():
    assert {golden.agent for golden in GOLDEN_QUESTIONS} == {"context-agent", "risk-agent", "coordinator-agent"}


@pytest.mark.parametrize("golden", GOLDEN_QUESTIONS, ids=lambda golden: golden.question[:40])
def test_every_tool_a_golden_question_expects_is_named_in_its_agents_prompt(golden):
    prompt = (PROMPTS_DIR / f"{golden.agent}.md").read_text()
    assert golden.tools and all(tool in prompt for tool in golden.tools)


@pytest.mark.parametrize("golden", [golden for golden in GOLDEN_QUESTIONS if golden.payment], ids=lambda golden: golden.agent)
def test_a_payment_question_writes_every_payment_value_as_the_tool_takes_it(golden):
    assert all(str(value) in golden.question for value in PAYMENT.values())
