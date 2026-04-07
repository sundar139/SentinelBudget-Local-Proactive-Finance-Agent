from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID

from sentinelbudget.config import Settings
from sentinelbudget.db.repositories.session import transaction
from sentinelbudget.logging import setup_logging
from sentinelbudget.review.models import ReviewMode
from sentinelbudget.review.service import ProactiveReviewService


@dataclass(frozen=True, slots=True)
class DaemonRunRecord:
    mode: ReviewMode
    user_id: UUID
    created_count: int
    skipped_count: int
    generated_at: datetime
    error: str | None = None


@dataclass(slots=True)
class ReviewDaemon:
    settings: Settings
    review_service: ProactiveReviewService
    user_ids: list[UUID]
    _last_daily_bucket: str | None = None
    _last_weekly_bucket: str | None = None
    _stop_event: threading.Event = field(default_factory=threading.Event, repr=False)

    def __post_init__(self) -> None:
        if not self.user_ids:
            raise ValueError("user_ids cannot be empty")

    def stop(self) -> None:
        self._stop_event.set()

    def run_once(
        self,
        mode: ReviewMode,
        now: datetime | None = None,
    ) -> list[DaemonRunRecord]:
        generated_at = now.astimezone(UTC) if now else datetime.now(UTC)
        records: list[DaemonRunRecord] = []

        for user_id in self.user_ids:
            try:
                with transaction(self.settings) as conn:
                    outcome = self.review_service.run_review(
                        conn,
                        user_id=user_id,
                        mode=mode,
                        reference_time=generated_at,
                        persist=True,
                    )
                records.append(
                    DaemonRunRecord(
                        mode=mode,
                        user_id=user_id,
                        created_count=len(outcome.created_insight_ids),
                        skipped_count=len(outcome.skipped_fingerprints),
                        generated_at=generated_at,
                    )
                )
            except Exception as exc:
                records.append(
                    DaemonRunRecord(
                        mode=mode,
                        user_id=user_id,
                        created_count=0,
                        skipped_count=0,
                        generated_at=generated_at,
                        error=str(exc),
                    )
                )

        return records

    def run_pending(self, now: datetime | None = None) -> list[DaemonRunRecord]:
        current = now.astimezone(UTC) if now else datetime.now(UTC)
        out: list[DaemonRunRecord] = []

        daily_bucket = current.date().isoformat()
        if (
            current.hour >= self.settings.review_daily_hour_utc
            and self._last_daily_bucket != daily_bucket
        ):
            out.extend(self.run_once(mode="daily", now=current))
            self._last_daily_bucket = daily_bucket

        iso_year, iso_week, _ = current.isocalendar()
        weekly_bucket = f"{iso_year}-W{iso_week:02d}"
        weekly_due = (
            current.weekday() == self.settings.review_weekly_day_utc
            and current.hour >= self.settings.review_weekly_hour_utc
            and self._last_weekly_bucket != weekly_bucket
        )
        if weekly_due:
            out.extend(self.run_once(mode="weekly", now=current))
            self._last_weekly_bucket = weekly_bucket

        return out

    def run_forever(self) -> None:
        logger = setup_logging(self.settings.log_level)
        logger.info(
            "Starting proactive review daemon",
            extra={
                "user_count": len(self.user_ids),
                "daily_hour_utc": self.settings.review_daily_hour_utc,
                "weekly_day_utc": self.settings.review_weekly_day_utc,
                "weekly_hour_utc": self.settings.review_weekly_hour_utc,
                "poll_seconds": self.settings.review_daemon_poll_seconds,
            },
        )

        try:
            while not self._stop_event.is_set():
                try:
                    run_records = self.run_pending()
                except Exception as exc:
                    logger.error(
                        "Review daemon run_pending failed",
                        extra={"detail": str(exc)},
                    )
                    self._stop_event.wait(timeout=self.settings.review_daemon_poll_seconds)
                    continue

                for record in run_records:
                    if record.error is not None:
                        logger.warning(
                            "Review daemon job failed",
                            extra={
                                "mode": record.mode,
                                "user_id": str(record.user_id),
                                "detail": record.error,
                                "generated_at": record.generated_at.isoformat(),
                            },
                        )
                        continue

                    logger.info(
                        "Review daemon job completed",
                        extra={
                            "mode": record.mode,
                            "user_id": str(record.user_id),
                            "created_count": record.created_count,
                            "skipped_count": record.skipped_count,
                            "generated_at": record.generated_at.isoformat(),
                        },
                    )

                self._stop_event.wait(timeout=self.settings.review_daemon_poll_seconds)
        except KeyboardInterrupt:  # pragma: no cover
            logger.info("Review daemon interrupted by keyboard signal")
        finally:
            logger.info("Review daemon stopped")
