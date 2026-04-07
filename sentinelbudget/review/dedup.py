from __future__ import annotations

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sentinelbudget.review.models import ReviewMode


def build_time_bucket(mode: ReviewMode, generated_at: datetime) -> str:
    if mode == "daily":
        return generated_at.date().isoformat()

    iso_year, iso_week, _ = generated_at.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def build_evidence_signature(evidence: dict[str, Any]) -> str:
    normalized = _normalize(evidence)
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_insight_fingerprint(
    user_id: UUID,
    kind: str,
    severity: str,
    evidence_signature: str,
    time_bucket: str,
) -> str:
    payload = "|".join([str(user_id), kind, severity, evidence_signature, time_bucket])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return f"{value:.4f}"
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    if isinstance(value, dict):
        return {key: _normalize(value[key]) for key in sorted(value.keys())}
    return value
