from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal, cast
from uuid import UUID

ReviewMode = Literal["daily", "weekly"]
ReviewSeverity = Literal["low", "medium", "high"]


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
class ReviewFinding:
    kind: str
    severity: ReviewSeverity
    summary: str
    evidence: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ReviewResult:
    user_id: UUID
    mode: ReviewMode
    generated_at: datetime
    findings: list[ReviewFinding]
    warnings: list[str]
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return cast(dict[str, Any], _serialize(asdict(self)))


@dataclass(frozen=True, slots=True)
class InsightDraft:
    kind: str
    severity: str
    title: str
    body: str
    details: dict[str, Any]
    fingerprint: str


@dataclass(frozen=True, slots=True)
class ReviewRunOutcome:
    review: ReviewResult
    drafts: list[InsightDraft]
    created_insight_ids: list[int]
    skipped_fingerprints: list[str]

    def to_dict(self) -> dict[str, Any]:
        return cast(dict[str, Any], _serialize(asdict(self)))
