from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from sentinelbudget.agent.models import (
    ChatMessage,
    ChatModelResult,
    ChatToolCall,
    ChatToolDefinition,
)


class ChatProviderError(RuntimeError):
    """Base error for chat provider failures."""


class ChatProviderUnavailableError(ChatProviderError):
    """Raised when the local model endpoint cannot be reached."""


class ChatProviderResponseError(ChatProviderError):
    """Raised when a model response is malformed."""


class ChatModelProvider(Protocol):
    def chat(
        self,
        messages: list[ChatMessage],
        tools: list[ChatToolDefinition],
    ) -> ChatModelResult:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class OllamaChatModelProvider:
    base_url: str
    model: str
    timeout_seconds: int = 30
    temperature: float = 0.0

    def __post_init__(self) -> None:
        if self.base_url.strip() == "":
            raise ValueError("base_url cannot be empty")
        if self.model.strip() == "":
            raise ValueError("model cannot be empty")
        if self.timeout_seconds < 1:
            raise ValueError("timeout_seconds must be positive")
        if self.temperature < 0.0 or self.temperature > 1.0:
            raise ValueError("temperature must be between 0.0 and 1.0")

    def chat(
        self,
        messages: list[ChatMessage],
        tools: list[ChatToolDefinition],
    ) -> ChatModelResult:
        if not messages:
            raise ValueError("messages cannot be empty")

        payload = {
            "model": self.model,
            "stream": False,
            "messages": [_message_to_payload(item) for item in messages],
            "tools": [_tool_to_payload(item) for item in tools],
            "options": {
                "temperature": self.temperature,
            },
        }

        request = Request(
            url=self.base_url.rstrip("/") + "/api/chat",
            data=json.dumps(payload, sort_keys=True).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                body_text = response.read().decode("utf-8")
        except HTTPError as exc:
            raise ChatProviderError(f"Ollama chat request failed: {exc.code} {exc.reason}") from exc
        except URLError as exc:
            raise ChatProviderUnavailableError(f"Ollama chat unavailable: {exc.reason}") from exc

        try:
            payload_obj = json.loads(body_text)
        except json.JSONDecodeError as exc:
            raise ChatProviderResponseError("Ollama response was not valid JSON") from exc

        message_obj = payload_obj.get("message")
        if not isinstance(message_obj, dict):
            raise ChatProviderResponseError("Ollama response missing 'message' object")

        content_obj = message_obj.get("content")
        if content_obj is not None and not isinstance(content_obj, str):
            raise ChatProviderResponseError("Ollama response content must be a string")

        tool_call = _parse_tool_call(message_obj)
        return ChatModelResult(content=content_obj, tool_call=tool_call)


@dataclass(slots=True)
class FakeDeterministicChatModelProvider:
    scripted_responses: list[ChatModelResult]
    errors_by_call: dict[int, Exception] = field(default_factory=dict)
    call_count: int = 0

    def chat(
        self,
        messages: list[ChatMessage],
        tools: list[ChatToolDefinition],
    ) -> ChatModelResult:
        del messages, tools

        self.call_count += 1
        if self.call_count in self.errors_by_call:
            raise self.errors_by_call[self.call_count]

        if not self.scripted_responses:
            raise ChatProviderResponseError("No scripted model responses remaining")

        return self.scripted_responses.pop(0)


def _message_to_payload(message: ChatMessage) -> dict[str, object]:
    payload: dict[str, object] = {
        "role": message.role,
        "content": message.content,
    }
    if message.name is not None:
        payload["name"] = message.name
    if message.tool_call_id is not None:
        payload["tool_call_id"] = message.tool_call_id
    return payload


def _tool_to_payload(tool: ChatToolDefinition) -> dict[str, object]:
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.input_schema,
        },
    }


def _parse_tool_call(message_obj: dict[str, object]) -> ChatToolCall | None:
    raw_calls = message_obj.get("tool_calls")
    if raw_calls is None:
        return None

    if not isinstance(raw_calls, list):
        raise ChatProviderResponseError("tool_calls must be a list")
    if len(raw_calls) == 0:
        return None

    raw_call = raw_calls[0]
    if not isinstance(raw_call, dict):
        raise ChatProviderResponseError("tool_call entry must be an object")

    function_obj = raw_call.get("function")
    if not isinstance(function_obj, dict):
        raise ChatProviderResponseError("tool_call.function must be an object")

    name_obj = function_obj.get("name")
    if not isinstance(name_obj, str) or name_obj.strip() == "":
        raise ChatProviderResponseError("tool_call.function.name must be a non-empty string")

    args_obj = function_obj.get("arguments", {})
    if isinstance(args_obj, str):
        try:
            args_obj = json.loads(args_obj)
        except json.JSONDecodeError as exc:
            raise ChatProviderResponseError(
                "tool_call.function.arguments was not valid JSON"
            ) from exc

    if not isinstance(args_obj, dict):
        raise ChatProviderResponseError("tool_call.function.arguments must decode to an object")

    call_id_obj = raw_call.get("id")
    call_id = call_id_obj if isinstance(call_id_obj, str) else None

    return ChatToolCall(name=name_obj, arguments=args_obj, call_id=call_id)
