from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sentinelbudget.review.dedup import (
    build_evidence_signature,
    build_insight_fingerprint,
    build_time_bucket,
)


def test_evidence_signature_is_deterministic() -> None:
    first = build_evidence_signature({"b": 2, "a": 1})
    second = build_evidence_signature({"a": 1, "b": 2})

    assert first == second


def test_fingerprint_changes_with_bucket_and_evidence() -> None:
    user_id = uuid4()
    evidence_a = build_evidence_signature({"score": "0.90"})
    evidence_b = build_evidence_signature({"score": "0.95"})

    bucket_daily = build_time_bucket("daily", datetime(2026, 4, 6, 7, tzinfo=UTC))
    bucket_weekly = build_time_bucket("weekly", datetime(2026, 4, 6, 7, tzinfo=UTC))

    fp_a_daily = build_insight_fingerprint(
        user_id,
        "anomaly_event",
        "high",
        evidence_a,
        bucket_daily,
    )
    fp_b_daily = build_insight_fingerprint(
        user_id,
        "anomaly_event",
        "high",
        evidence_b,
        bucket_daily,
    )
    fp_a_weekly = build_insight_fingerprint(
        user_id,
        "anomaly_event",
        "high",
        evidence_a,
        bucket_weekly,
    )

    assert fp_a_daily != fp_b_daily
    assert fp_a_daily != fp_a_weekly
