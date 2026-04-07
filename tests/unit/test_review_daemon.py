from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sentinelbudget.review.daemon import ReviewDaemon


class _FakeReviewService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def run_review(self, conn, user_id, mode, reference_time, persist):
        del conn, reference_time, persist
        self.calls.append((str(user_id), mode))
        return SimpleNamespace(created_insight_ids=[1], skipped_fingerprints=[])


class _FlakyReviewService:
    def __init__(self, fail_user_id: str) -> None:
        self._fail_user_id = fail_user_id

    def run_review(self, conn, user_id, mode, reference_time, persist):
        del conn, mode, reference_time, persist
        if str(user_id) == self._fail_user_id:
            raise RuntimeError("simulated review failure")
        return SimpleNamespace(created_insight_ids=[2], skipped_fingerprints=[])


@contextmanager
def _fake_transaction(settings):
    del settings
    yield object()


def test_daemon_run_pending_triggers_once_per_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sentinelbudget.review.daemon.transaction", _fake_transaction)

    user_id = uuid4()
    settings = SimpleNamespace(
        log_level="INFO",
        review_daily_hour_utc=7,
        review_weekly_day_utc=0,
        review_weekly_hour_utc=8,
        review_daemon_poll_seconds=1,
    )

    daemon = ReviewDaemon(
        settings=settings,
        review_service=_FakeReviewService(),
        user_ids=[user_id],
    )

    first = daemon.run_pending(datetime(2026, 4, 6, 7, 0, tzinfo=UTC))
    second = daemon.run_pending(datetime(2026, 4, 6, 7, 30, tzinfo=UTC))

    assert len(first) == 1
    assert len(second) == 0


def test_daemon_weekly_trigger_runs_when_due(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sentinelbudget.review.daemon.transaction", _fake_transaction)

    user_id = uuid4()
    settings = SimpleNamespace(
        log_level="INFO",
        review_daily_hour_utc=23,
        review_weekly_day_utc=0,
        review_weekly_hour_utc=8,
        review_daemon_poll_seconds=1,
    )

    daemon = ReviewDaemon(
        settings=settings,
        review_service=_FakeReviewService(),
        user_ids=[user_id],
    )

    runs = daemon.run_pending(datetime(2026, 4, 6, 8, 5, tzinfo=UTC))
    assert len(runs) == 1
    assert runs[0].mode == "weekly"


def test_daemon_run_once_isolates_user_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sentinelbudget.review.daemon.transaction", _fake_transaction)

    failing = uuid4()
    passing = uuid4()
    settings = SimpleNamespace(
        log_level="INFO",
        review_daily_hour_utc=7,
        review_weekly_day_utc=0,
        review_weekly_hour_utc=8,
        review_daemon_poll_seconds=1,
    )

    daemon = ReviewDaemon(
        settings=settings,
        review_service=_FlakyReviewService(fail_user_id=str(failing)),
        user_ids=[failing, passing],
    )

    records = daemon.run_once("daily", now=datetime(2026, 4, 6, 7, 0, tzinfo=UTC))
    assert len(records) == 2

    failed_records = [item for item in records if item.user_id == failing]
    passed_records = [item for item in records if item.user_id == passing]

    assert len(failed_records) == 1
    assert failed_records[0].error is not None
    assert len(passed_records) == 1
    assert passed_records[0].created_count == 1


def test_daemon_requires_at_least_one_user_id() -> None:
    settings = SimpleNamespace(
        log_level="INFO",
        review_daily_hour_utc=7,
        review_weekly_day_utc=0,
        review_weekly_hour_utc=8,
        review_daemon_poll_seconds=1,
    )

    with pytest.raises(ValueError, match="user_ids cannot be empty"):
        ReviewDaemon(
            settings=settings,
            review_service=_FakeReviewService(),
            user_ids=[],
        )
