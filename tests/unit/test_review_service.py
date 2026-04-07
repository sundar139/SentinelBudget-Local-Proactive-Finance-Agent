from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from sentinelbudget.review.service import ProactiveReviewService
from sentinelbudget.review.service import main as review_main


class _FakeMemoryService:
    def query_memory(self, conn, user_id, query_text, top_k):
        del conn, user_id, query_text, top_k
        return []


class _InMemoryInsightRepo:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}
        self._id = 1

    def create_if_new_unread(
        self,
        conn,
        user_id,
        kind,
        title,
        body,
        severity,
        details,
        fingerprint,
    ):
        del conn, kind, title, body, severity, details
        key = f"{user_id}:{fingerprint}"
        if key in self.rows:
            return SimpleNamespace(id=self.rows[key]["id"]), False

        self.rows[key] = {"id": self._id, "read": False}
        self._id += 1
        return SimpleNamespace(id=self.rows[key]["id"]), True

    def list_by_user(self, conn, user_id, limit=100, unread_only=False):
        del conn, user_id, limit, unread_only
        return []

    def list_unread(self, conn, user_id, limit=100):
        del conn, user_id, limit
        return []

    def mark_read(self, conn, user_id, insight_id):
        del conn, user_id, insight_id
        return True

    def mark_many_read(self, conn, user_id, insight_ids):
        del conn, user_id
        return len(insight_ids)


class _FakeAnalyticsResult:
    def __init__(self, score: str) -> None:
        self.anomaly_events = [SimpleNamespace()]
        self.recurring_candidates = []
        self._score = score

    def to_dict(self):
        return {
            "kpis": {
                "total_income": "1000.00",
                "total_expenses": "1200.00",
                "net_cashflow": "-200.00",
                "top_spending_categories": [
                    {
                        "category_name": "Dining",
                        "total_spend": "300.00",
                        "transaction_count": 5,
                    }
                ],
            },
            "anomaly_events": [
                {
                    "anomaly_kind": "spike",
                    "severity": "high",
                    "score": self._score,
                    "trans_key": 123,
                    "ts": "2026-04-05T00:00:00+00:00",
                    "explanation": "Large spend spike",
                }
            ],
            "recurring_candidates": [],
        }


def test_review_service_is_idempotent_for_same_evidence(monkeypatch):
    settings = SimpleNamespace(review_memory_top_k=5)
    repo = _InMemoryInsightRepo()

    def fake_run_analytics(**kwargs):
        del kwargs
        return _FakeAnalyticsResult("0.90")

    monkeypatch.setattr("sentinelbudget.review.service.run_analytics", fake_run_analytics)
    monkeypatch.setattr(
        "sentinelbudget.review.service.GoalRepository.list_by_user",
        lambda conn, user_id, limit: [],
    )

    service = ProactiveReviewService(
        settings=settings,
        memory_service=_FakeMemoryService(),
        insight_repository=repo,
    )

    user_id = uuid4()
    now = datetime(2026, 4, 6, 10, tzinfo=UTC)

    first = service.run_review(conn=object(), user_id=user_id, mode="daily", reference_time=now)
    second = service.run_review(conn=object(), user_id=user_id, mode="daily", reference_time=now)

    assert len(first.created_insight_ids) > 0
    assert len(second.created_insight_ids) == 0
    assert len(second.skipped_fingerprints) > 0


def test_review_service_creates_new_insight_when_evidence_changes(monkeypatch):
    settings = SimpleNamespace(review_memory_top_k=5)
    repo = _InMemoryInsightRepo()
    score = {"value": "0.90"}

    def fake_run_analytics(**kwargs):
        del kwargs
        return _FakeAnalyticsResult(score["value"])

    monkeypatch.setattr("sentinelbudget.review.service.run_analytics", fake_run_analytics)
    monkeypatch.setattr(
        "sentinelbudget.review.service.GoalRepository.list_by_user",
        lambda conn, user_id, limit: [],
    )

    service = ProactiveReviewService(
        settings=settings,
        memory_service=_FakeMemoryService(),
        insight_repository=repo,
    )

    user_id = uuid4()
    now = datetime(2026, 4, 6, 10, tzinfo=UTC)

    first = service.run_review(conn=object(), user_id=user_id, mode="daily", reference_time=now)
    score["value"] = "0.95"
    second = service.run_review(conn=object(), user_id=user_id, mode="daily", reference_time=now)

    assert len(first.created_insight_ids) > 0
    assert len(second.created_insight_ids) > 0


def test_review_help_does_not_require_settings(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["sentinelbudget-review", "--help"])

    def _fail_if_called():
        raise AssertionError("get_settings should not be called for --help")

    monkeypatch.setattr("sentinelbudget.review.service.get_settings", _fail_if_called)

    with pytest.raises(SystemExit) as exc_info:
        review_main()

    assert exc_info.value.code == 0
