"""Asking a kagent agent a question over A2A, and telling whether its answer quotes the tool's numbers.

kagent's controller serves every agent's A2A endpoint at /api/a2a/<namespace>/<agent>/.
A question is a JSON-RPC message/send; with blocking set, the answer comes back
once the agent has finished, as a task whose artifacts hold the text and whose
history holds the messages it sent on the way, tool calls among them.
"""

import re
from collections.abc import Iterable
from typing import NamedTuple
from uuid import uuid4

import httpx

# How far a number in an answer may be from the tool's value and still count as quoting it.
TOLERANCE_PERCENT = 1
# A number with thousands separators, or a plain one. Digits right after a letter,
# an underscore or a dot belong to something else, such as cust_00090571 or v2.5.
NUMBER = re.compile(r"(?<![\w.])-?\d{1,3}(?:,\d{3})+(?:\.\d+)?|(?<![\w.,])-?\d+(?:\.\d+)?")


class A2AError(Exception):
    """The agent could not be asked: an HTTP error or a JSON-RPC error."""


class Answer(NamedTuple):
    """How the agent's answer ended, its text, and the tools it called on the way, in order."""

    state: str
    text: str
    tools: list[str]


def message_request(text: str, request_id: str) -> dict:
    """A JSON-RPC message/send that waits for the agent's whole answer."""

    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "message/send",
        "params": {
            "message": {"kind": "message", "role": "user", "messageId": request_id, "parts": [{"kind": "text", "text": text}]},
            "configuration": {"blocking": True},
        },
    }


def _texts(parts: list[dict]) -> list[str]:
    return [part["text"] for part in parts if part.get("kind") == "text"]


def answer_text(result: dict) -> str:
    """The text the agent sent: a message's parts, or a task's artifacts, or else its status message."""

    if result.get("kind") == "message":
        return "\n".join(_texts(result.get("parts", [])))
    texts = [text for artifact in result.get("artifacts", []) for text in _texts(artifact.get("parts", []))]
    if not texts:
        texts = _texts(result.get("status", {}).get("message", {}).get("parts", []))
    return "\n".join(texts)


def task_state(result: dict) -> str:
    """How the answer ended: a task's state, and completed for a plain message."""

    if result.get("kind") == "message":
        return "completed"
    return result["status"]["state"]


def _is_call(part: dict) -> bool:
    metadata = part.get("metadata") or {}
    return part.get("kind") == "data" and "function_call" in (metadata.get("kagent_type"), metadata.get("adk_type"))


def tool_calls(result: dict) -> list[str]:
    """The tools the agent called, in order and with repeats: from its history, then its status message."""

    if result.get("kind") == "message":
        messages = [result]
    else:
        messages = [*result.get("history", []), result.get("status", {}).get("message", {})]
    return [part["data"]["name"]
            for message in messages
            for part in message.get("parts", []) if _is_call(part)]


async def ask(client: httpx.AsyncClient, url: str, text: str) -> Answer:
    """Ask the agent at url one question; how the answer ended, its text and its tool calls."""

    response = await client.post(url, json=message_request(text, uuid4().hex))
    if response.status_code != 200:
        raise A2AError(f"{response.status_code}: {response.text[:200]}")
    body = response.json()
    if "error" in body:
        raise A2AError(body["error"].get("message", str(body["error"])))
    result = body["result"]
    return Answer(task_state(result), answer_text(result), tool_calls(result))


def numbers(text: str) -> list[float]:
    """Every number written in the text, thousands separators removed."""

    return [float(found.replace(",", "")) for found in NUMBER.findall(text)]


def close(number: float, value: float) -> bool:
    """Whether a number in an answer is within 1% of the tool's value."""

    # Multiplied out, so the 1% boundary is exact for whole numbers instead of depending on float rounding.
    return abs(number - value) * 100 <= TOLERANCE_PERCENT * abs(value)


def grounded(answer: str, values: Iterable[float | int]) -> bool:
    """Whether the answer quotes at least one of the tool's values of 1 or more, within 1%."""

    # Values below 1, such as rates, are too easy to match by chance.
    quoted = numbers(answer)
    return any(close(number, value) for value in values if abs(value) >= 1 for number in quoted)
