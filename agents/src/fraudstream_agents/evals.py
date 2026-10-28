"""The golden questions: what an analyst asks each agent, the tool calls it must make and what its answer must hold.

The truth is read when the check runs: a customer's or merchant's stored values and
the handbook's chunks from the context API, and the model's answer for the payment
from the inference API.
"""

import re
from dataclasses import dataclass, replace

import httpx

from fraudstream_agents.a2a import Answer, close, grounded, numbers

PAYMENT = {
    "transaction_id": "txn_000000128913",
    "customer_id": "cust_00090571",
    "merchant_id": "merch_00011503",
    "event_timestamp": "2026-06-22T23:59:21Z",
    "amount": 143.92,
    "channel": "online",
    "city": "New York",
}
DECISION = re.compile(r"model decision:\s*(not flagged|flagged)", re.IGNORECASE)
CHUNK = re.compile(r"\[([a-z][\w./-]*#[\w.-]+)\]")


@dataclass(frozen=True)
class GoldenQuestion:
    """One question to one agent.

    handbook is the prefix a cited chunk must start with, "" for any chunk and None
    when no citation is wanted; payment means the answer must report the model's
    decision on PAYMENT.
    """

    agent: str
    question: str
    tools: frozenset[str]
    customer: str | None = None
    merchant: str | None = None
    payment: bool = False
    handbook: str | None = None


_PAYMENT_TEXT = "2026-06-22T23:59:21Z, 143.92 USD, online, New York"
# Each agent's first question is the one --load sends.
GOLDEN_QUESTIONS = (
    GoldenQuestion("context-agent", "What has customer cust_00090571 been doing recently?",
                   frozenset({"get_customer_features"}), customer="cust_00090571"),
    GoldenQuestion("context-agent", "How risky is merchant merch_00011503 right now?",
                   frozenset({"get_merchant_features"}), merchant="merch_00011503"),
    GoldenQuestion("risk-agent",
                   f"Payment txn_000000128913: customer cust_00090571, merchant merch_00011503, {_PAYMENT_TEXT}. "
                   "What does the model say, and what should I do?",
                   frozenset({"score_payment", "search_handbook"}), payment=True, handbook=""),
    GoldenQuestion("risk-agent", "Who has to agree before a payment above 5,000 USD is blocked?",
                   frozenset({"search_handbook"}), handbook="handbook/review-policy"),
    GoldenQuestion("coordinator-agent", "Who has to agree before a payment above 5,000 USD is blocked?",
                   frozenset({"kagent__NS__risk_agent"}), handbook="handbook/review-policy"),
    GoldenQuestion("coordinator-agent",
                   f"Customer cust_00090571 made payment txn_000000128913 at merchant merch_00011503, {_PAYMENT_TEXT}. "
                   "What has the customer been doing, what does the model say, and what should I do?",
                   frozenset({"kagent__NS__context_agent", "kagent__NS__risk_agent"}),
                   customer="cust_00090571", payment=True, handbook=""),
)

# An agent made from another agent's manifest on a different model config, asked that agent's questions.
VARIANTS = {"risk-agent-b": "risk-agent"}


def questions_for(agents: list[str] | None) -> list[GoldenQuestion]:
    """The named agents' questions in order, a variant asked its base agent's; every agent's but no variant's when none is named."""

    if not agents:
        return list(GOLDEN_QUESTIONS)
    return [replace(golden, agent=agent)
            for agent in agents
            for golden in GOLDEN_QUESTIONS if golden.agent == VARIANTS.get(agent, agent)]


def decision(text: str) -> bool | None:
    """The model's decision from the answer's last "Model decision:" line: flagged or not, None without one."""

    found = DECISION.findall(text)
    return found[-1].lower() == "flagged" if found else None


def cited_chunks(text: str) -> list[str]:
    """The chunk ids the answer cites in square brackets, in order, each once."""

    return list(dict.fromkeys(CHUNK.findall(text)))


def score_reported(text: str, prediction: dict) -> bool:
    """Whether the answer carries the model's decision and quotes its threshold; with no threshold, no decision line."""

    if prediction["threshold"] is None:
        return decision(text) is None
    return decision(text) == prediction["flagged"] and any(close(number, prediction["threshold"]) for number in numbers(text))


def stored_values(body: dict) -> list[float]:
    """Every numeric feature value the context API returned, in all views."""

    return [
        value
        for view in body["views"].values()
        for value in view["features"].values()
        if isinstance(value, int | float) and not isinstance(value, bool)
    ]


async def problems(client: httpx.AsyncClient, golden_question: GoldenQuestion, answer: Answer, context_api: str, inference_api: str) -> list[str]:
    """Every way the answer falls short of the golden question; empty when it passes.

    A truth request that fails raises: then the check's surroundings are broken, not the answer.
    """

    found = []
    if answer.state != "completed":
        found.append(f"the answer ended {answer.state}")
    found += [f"did not call {tool}" for tool in sorted(golden_question.tools - set(answer.tools))]
    for entity_id, kind in ((golden_question.customer, "customers"), (golden_question.merchant, "merchants")):
        if entity_id:
            truth = await client.get(f"{context_api}/v1/{kind}/{entity_id}")
            truth.raise_for_status()
            if not grounded(answer.text, stored_values(truth.json())):
                found.append(f"quotes none of {entity_id}'s stored values")
    if golden_question.payment:
        truth = await client.post(f"{inference_api}/v1/predict", json=PAYMENT)
        truth.raise_for_status()
        if not score_reported(answer.text, truth.json()):
            found.append(f"does not report the model's decision and threshold for {PAYMENT['transaction_id']}")
    elif decision(answer.text) is not None:
        found.append("gives a model decision, but no payment was scored")
    if golden_question.handbook is not None:
        found += await _citation_problems(client, cited_chunks(answer.text), golden_question.handbook, context_api)
    return found


async def _citation_problems(client: httpx.AsyncClient, cited: list[str], prefix: str, context_api: str) -> list[str]:
    if not cited:
        return ["cites no chunk"]
    found, existing = [], []
    for chunk_id in cited:
        truth = await client.get(f"{context_api}/v1/chunks", params={"chunk_id": chunk_id})
        if truth.status_code == 404:
            found.append(f"cites {chunk_id}, which does not exist")
            continue
        truth.raise_for_status()
        existing.append(chunk_id)
    if existing and not any(chunk_id.startswith(prefix) for chunk_id in existing):
        found.append(f"cites nothing under {prefix}")
    return found
