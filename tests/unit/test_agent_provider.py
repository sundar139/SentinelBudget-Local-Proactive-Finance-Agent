from __future__ import annotations

import json
from email.message import Message
from typing import Any
from urllib.error import HTTPError, URLError

import pytest
from sentinelbudget.agent.models import ChatMessage, ChatModelResult, ChatToolCall
from sentinelbudget.agent.provider import (
    ChatProviderError,
    ChatProviderResponseError,
    ChatProviderUnavailableError,
    FakeDeterministicChatModelProvider,
    OllamaChatModelProvider,
)


class _FakeHTTPResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def __enter__(self) -> _FakeHTTPResponse:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc_type, exc, traceback

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


def test_fake_provider_returns_scripted_response() -> None:
    provider = FakeDeterministicChatModelProvider(
        scripted_responses=[
            ChatModelResult(
                content=None,
                tool_call=ChatToolCall(name="get_kpi_summary", arguments={}),
            )
        ]
    )

    result = provider.chat(messages=[ChatMessage(role="user", content="hello")], tools=[])

    assert result.tool_call is not None
    assert result.tool_call.name == "get_kpi_summary"
    assert provider.call_count == 1


def test_ollama_provider_parses_tool_call(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "message": {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {
                    "id": "call-1",
                    "function": {
                        "name": "search_semantic_memory",
                        "arguments": {"query_text": "dining spend", "top_k": 3},
                    },
                }
            ],
        }
    }

    def fake_urlopen(request: object, timeout: int) -> _FakeHTTPResponse:
        del request, timeout
        return _FakeHTTPResponse(payload)

    monkeypatch.setattr("sentinelbudget.agent.provider.urlopen", fake_urlopen)

    provider = OllamaChatModelProvider(base_url="http://localhost:11434", model="llama3.1")
    result = provider.chat(messages=[ChatMessage(role="user", content="help")], tools=[])

    assert result.tool_call is not None
    assert result.tool_call.name == "search_semantic_memory"
    assert result.tool_call.arguments["query_text"] == "dining spend"


def test_ollama_provider_rejects_invalid_tool_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "message": {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {
                    "function": {
                        "name": "get_kpi_summary",
                        "arguments": "{not valid json}",
                    }
                }
            ],
        }
    }

    def fake_urlopen(request: object, timeout: int) -> _FakeHTTPResponse:
        del request, timeout
        return _FakeHTTPResponse(payload)

    monkeypatch.setattr("sentinelbudget.agent.provider.urlopen", fake_urlopen)

    provider = OllamaChatModelProvider(base_url="http://localhost:11434", model="llama3.1")

    with pytest.raises(ChatProviderResponseError, match="arguments"):
        provider.chat(messages=[ChatMessage(role="user", content="hello")], tools=[])


def test_ollama_provider_uses_first_tool_call_when_multiple_returned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {
        "message": {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {
                    "id": "call-1",
                    "function": {
                        "name": "get_kpi_summary",
                        "arguments": {"window": "last_30_days"},
                    },
                },
                {
                    "id": "call-2",
                    "function": {
                        "name": "get_anomalies",
                        "arguments": {"window": "last_30_days"},
                    },
                },
            ],
        }
    }

    def fake_urlopen(request: object, timeout: int) -> _FakeHTTPResponse:
        del request, timeout
        return _FakeHTTPResponse(payload)

    monkeypatch.setattr("sentinelbudget.agent.provider.urlopen", fake_urlopen)

    provider = OllamaChatModelProvider(base_url="http://localhost:11434", model="llama3.1")
    result = provider.chat(messages=[ChatMessage(role="user", content="help")], tools=[])

    assert result.tool_call is not None
    assert result.tool_call.name == "get_kpi_summary"


def test_ollama_provider_classifies_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_urlopen(request: object, timeout: int) -> _FakeHTTPResponse:
        del timeout
        raise HTTPError(
            url="http://localhost:11434/api/chat",
            code=500,
            msg="Internal Server Error",
            hdrs=Message(),
            fp=None,
        )

    monkeypatch.setattr("sentinelbudget.agent.provider.urlopen", fake_urlopen)

    provider = OllamaChatModelProvider(base_url="http://localhost:11434", model="llama3.1")
    with pytest.raises(ChatProviderError, match="500"):
        provider.chat(messages=[ChatMessage(role="user", content="hello")], tools=[])


def test_ollama_provider_classifies_unavailable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_urlopen(request: object, timeout: int) -> _FakeHTTPResponse:
        del request, timeout
        raise URLError("connection refused")

    monkeypatch.setattr("sentinelbudget.agent.provider.urlopen", fake_urlopen)

    provider = OllamaChatModelProvider(base_url="http://localhost:11434", model="llama3.1")
    with pytest.raises(ChatProviderUnavailableError, match="unavailable"):
        provider.chat(messages=[ChatMessage(role="user", content="hello")], tools=[])
