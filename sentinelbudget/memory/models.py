from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal, cast
from uuid import UUID

MemoryKind = Literal["goal", "preference", "note"]


def _serialize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return f"{value:.2f}"
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
class SemanticMemoryItem:
    id: int
    user_id: UUID
    kind: MemoryKind
    text: str
    metadata: dict[str, Any]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class SemanticMemoryMatch:
    id: int
    user_id: UUID
    kind: MemoryKind
    text: str
    metadata: dict[str, Any]
    created_at: datetime
    score: Decimal


@dataclass(frozen=True, slots=True)
class MemorySyncSummary:
    kind: MemoryKind
    processed: int
    inserted: int
    updated: int
    skipped: int


@dataclass(frozen=True, slots=True)
class ContextGoal:
    goal_id: UUID
    title: str
    status: str
    target_amount: Decimal | None
    target_date: date | None


@dataclass(frozen=True, slots=True)
class ContextBundle:
    user_id: UUID
    query_text: str
    generated_at: datetime
    top_k: int
    memories: list[SemanticMemoryMatch]
    goals: list[ContextGoal]
    analytics_summary: dict[str, Any] | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return cast(dict[str, Any], _serialize(asdict(self)))
