from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sentinelbudget.review.generator import InsightGenerator
from sentinelbudget.review.models import ReviewFinding


class _FailingSummarizer:
    def summarize(self, finding, fallback_title, fallback_body):
        del finding, fallback_title, fallback_body
        raise RuntimeError("summarizer unavailable")


def test_generator_falls_back_to_deterministic_templates_on_summarizer_failure() -> None:
    finding = ReviewFinding(
        kind="anomaly_event",
        severity="high",
        summary="Large anomaly detected",
        evidence={"score": "0.95"},
    )

    generator = InsightGenerator(summarizer=_FailingSummarizer())
    drafts = generator.build_drafts(
        user_id=uuid4(),
        findings=[finding],
        mode="daily",
        generated_at=datetime(2026, 4, 6, 12, 0, tzinfo=UTC),
    )

    assert len(drafts) == 1
    assert drafts[0].title == "Unusual spending activity detected"
    assert "Large anomaly detected" in drafts[0].body
    assert drafts[0].fingerprint
