from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal, cast
from uuid import UUID

ConversationRole = Literal["system", "user", "assistant", "tool"]


def _serialize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return f"{value:.4f}"
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize(item) for key, item in value.items()}
    return value


@dataclass(frozen=True, slots=True)
class ConversationEntry:
    id: int
    session_id: UUID
    user_id: UUID
    role: ConversationRole
    content: str
    metadata: dict[str, Any]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ChatMessage:
    role: ConversationRole
    content: str
    name: str | None = None
    tool_call_id: str | None = None


@dataclass(frozen=True, slots=True)
class ChatToolDefinition:
    name: str
    description: str
    input_schema: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ChatToolCall:
    name: str
    arguments: dict[str, Any]
    call_id: str | None = None


@dataclass(frozen=True, slots=True)
class ChatModelResult:
    content: str | None
    tool_call: ChatToolCall | None


@dataclass(frozen=True, slots=True)
class ToolExecutionRecord:
    tool_name: str
    arguments: dict[str, Any]
    output: dict[str, Any]


@dataclass(frozen=True, slots=True)
class EvidenceBlock:
    tool_name: str
    evidence: str
    payload: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class GroundedAnswer:
    answer_text: str
    citations: list[EvidenceBlock]
    tools_used: list[str]
    warnings: list[str]
    structured_payload: dict[str, Any] | None
    session_id: UUID
    user_id: UUID
    created_at: datetime
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return cast(dict[str, Any], _serialize(asdict(self)))
