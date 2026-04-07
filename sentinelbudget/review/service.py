from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Protocol
from uuid import UUID

from psycopg import Connection

from sentinelbudget.analytics.service import run_analytics
from sentinelbudget.config import Settings, get_settings
from sentinelbudget.db.repositories.goals import Goal, GoalRepository
from sentinelbudget.db.repositories.insights import Insight, InsightRepository
from sentinelbudget.db.repositories.session import transaction
from sentinelbudget.logging import setup_logging
from sentinelbudget.memory.embeddings import OllamaEmbeddingProvider
from sentinelbudget.memory.service import SemanticMemoryService
from sentinelbudget.review.generator import InsightGenerator
from sentinelbudget.review.models import (
    ReviewFinding,
    ReviewMode,
    ReviewResult,
    ReviewRunOutcome,
    ReviewSeverity,
)


class InsightRepositoryProtocol(Protocol):
    def create_if_new_unread(
        self,
        conn: Connection,
        user_id: UUID,
        kind: str,
        title: str,
        body: str,
        severity: str,
        details: dict[str, Any],
        fingerprint: str,
    ) -> tuple[Insight | None, bool]:
        raise NotImplementedError

    def list_by_user(
        self,
        conn: Connection,
        user_id: UUID,
        limit: int = 100,
        unread_only: bool = False,
    ) -> list[Insight]:
        raise NotImplementedError

    def list_unread(
        self,
        conn: Connection,
        user_id: UUID,
        limit: int = 100,
    ) -> list[Insight]:
        raise NotImplementedError

    def mark_read(self, conn: Connection, user_id: UUID, insight_id: int) -> bool:
        raise NotImplementedError

    def mark_many_read(self, conn: Connection, user_id: UUID, insight_ids: list[int]) -> int:
        raise NotImplementedError


class ProactiveReviewService:
    """Deterministic proactive review and insight persistence service."""

    def __init__(
        self,
        settings: Settings,
        memory_service: SemanticMemoryService,
        insight_repository: InsightRepositoryProtocol = InsightRepository,
        insight_generator: InsightGenerator | None = None,
    ) -> None:
        self._settings = settings
        self._memory_service = memory_service
        self._insight_repository = insight_repository
        self._insight_generator = insight_generator or InsightGenerator()

    def run_review(
        self,
        conn: Connection,
        user_id: UUID,
        mode: ReviewMode,
        reference_time: datetime | None = None,
        persist: bool = True,
    ) -> ReviewRunOutcome:
        if mode not in {"daily", "weekly"}:
            raise ValueError("mode must be one of: daily, weekly")

        generated_at = reference_time.astimezone(UTC) if reference_time else datetime.now(UTC)
        primary_window = "last_7_days" if mode == "daily" else "last_30_days"

        primary = run_analytics(
            conn=conn,
            user_id=user_id,
            window=primary_window,
            reference_time=generated_at,
        )
        month_to_date = run_analytics(
            conn=conn,
            user_id=user_id,
            window="month_to_date",
            reference_time=generated_at,
        )

        warnings: list[str] = []
        memory_matches: list[dict[str, Any]] = []

        try:
            matches = self._memory_service.query_memory(
                conn,
                user_id=user_id,
                query_text="spending anomalies recurring goals cashflow",
                top_k=self._settings.review_memory_top_k,
            )
            memory_matches = [
                {
                    "id": item.id,
                    "kind": item.kind,
                    "score": f"{item.score:.4f}",
                    "text": item.text,
                }
                for item in matches
            ]
        except Exception as exc:
            warnings.append(f"semantic memory context unavailable: {exc}")

        goals = GoalRepository.list_by_user(conn, user_id, limit=10)

        findings = _collect_findings(
            primary=primary.to_dict(),
            month_to_date=month_to_date.to_dict(),
            goals=goals,
            now=generated_at,
        )

        review = ReviewResult(
            user_id=user_id,
            mode=mode,
            generated_at=generated_at,
            findings=findings,
            warnings=warnings,
            meta={
                "primary_window": primary_window,
                "primary_anomaly_count": len(primary.anomaly_events),
                "primary_recurring_count": len(primary.recurring_candidates),
                "memory_context_count": len(memory_matches),
                "goal_count": len(goals),
            },
        )

        drafts = self._insight_generator.build_drafts(
            user_id=user_id,
            findings=findings,
            mode=mode,
            generated_at=generated_at,
        )

        created_insight_ids: list[int] = []
        skipped_fingerprints: list[str] = []

        if persist:
            for draft in drafts:
                created, inserted = self._insight_repository.create_if_new_unread(
                    conn,
                    user_id=user_id,
                    kind=draft.kind,
                    title=draft.title,
                    body=draft.body,
                    severity=draft.severity,
                    details=draft.details,
                    fingerprint=draft.fingerprint,
                )
                if inserted and created is not None:
                    created_insight_ids.append(created.id)
                else:
                    skipped_fingerprints.append(draft.fingerprint)

        return ReviewRunOutcome(
            review=review,
            drafts=drafts,
            created_insight_ids=created_insight_ids,
            skipped_fingerprints=skipped_fingerprints,
        )

    def list_insights(self, conn: Connection, user_id: UUID, limit: int = 50) -> list[Insight]:
        return self._insight_repository.list_by_user(conn, user_id=user_id, limit=limit)

    def list_unread_insights(
        self,
        conn: Connection,
        user_id: UUID,
        limit: int = 50,
    ) -> list[Insight]:
        return self._insight_repository.list_unread(conn, user_id=user_id, limit=limit)

    def mark_insight_read(self, conn: Connection, user_id: UUID, insight_id: int) -> bool:
        return self._insight_repository.mark_read(conn, user_id=user_id, insight_id=insight_id)

    def mark_insights_read(
        self,
        conn: Connection,
        user_id: UUID,
        insight_ids: list[int],
    ) -> int:
        return self._insight_repository.mark_many_read(
            conn,
            user_id=user_id,
            insight_ids=insight_ids,
        )


def _collect_findings(
    primary: dict[str, Any],
    month_to_date: dict[str, Any],
    goals: list[Goal],
    now: datetime,
) -> list[ReviewFinding]:
    findings: list[ReviewFinding] = []

    anomaly_events = primary.get("anomaly_events", [])
    if isinstance(anomaly_events, list):
        for item in anomaly_events[:5]:
            if not isinstance(item, dict):
                continue
            findings.append(
                ReviewFinding(
                    kind="anomaly_event",
                    severity=_normalize_severity(item.get("severity")),
                    summary=str(item.get("explanation", "Anomaly detected")),
                    evidence={
                        "anomaly_kind": item.get("anomaly_kind"),
                        "severity": item.get("severity"),
                        "score": item.get("score"),
                        "trans_key": item.get("trans_key"),
                        "ts": item.get("ts"),
                    },
                )
            )

    kpis = primary.get("kpis", {})
    total_expenses = _to_decimal(kpis.get("total_expenses"))
    top_categories = kpis.get("top_spending_categories", [])
    if isinstance(top_categories, list) and total_expenses > Decimal("0.00"):
        for item in top_categories[:3]:
            if not isinstance(item, dict):
                continue
            spend = _to_decimal(item.get("total_spend"))
            if spend < Decimal("20.00"):
                continue

            share = (spend / total_expenses).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
            if share < Decimal("0.15"):
                continue

            spend_severity: ReviewSeverity = "high" if share >= Decimal("0.40") else "medium"
            category_name = str(item.get("category_name", "Uncategorized"))
            findings.append(
                ReviewFinding(
                    kind="category_spend_pressure",
                    severity=spend_severity,
                    summary=(
                        f"{category_name} spend is {spend:.2f}, about "
                        f"{(share * Decimal('100')).quantize(Decimal('0.01'))}% "
                        "of total expenses in the review window."
                    ),
                    evidence={
                        "category_name": category_name,
                        "total_spend": f"{spend:.2f}",
                        "expense_share": f"{share:.4f}",
                        "transaction_count": item.get("transaction_count"),
                    },
                )
            )

    recurring = primary.get("recurring_candidates", [])
    if isinstance(recurring, list):
        for item in recurring[:5]:
            if not isinstance(item, dict):
                continue
            expected_str = item.get("expected_next_date")
            if not isinstance(expected_str, str):
                continue

            try:
                expected = datetime.fromisoformat(expected_str).date()
            except ValueError:
                try:
                    expected = datetime.strptime(expected_str, "%Y-%m-%d").date()
                except ValueError:
                    continue

            days_overdue = (now.date() - expected).days
            if days_overdue <= 0:
                continue

            label = str(item.get("normalized_label", "unknown"))
            recurring_severity: ReviewSeverity = "high" if days_overdue > 14 else "medium"
            findings.append(
                ReviewFinding(
                    kind="recurring_overdue",
                    severity=recurring_severity,
                    summary=(
                        f"Recurring pattern '{label}' expected around {expected.isoformat()} "
                        f"is overdue by {days_overdue} day(s)."
                    ),
                    evidence={
                        "label": label,
                        "expected_next_date": expected.isoformat(),
                        "days_overdue": days_overdue,
                        "estimated_cadence": item.get("estimated_cadence"),
                        "median_amount": item.get("median_amount"),
                        "confidence": item.get("confidence"),
                    },
                )
            )

            lowered = label.lower()
            if "salary" in lowered or "payroll" in lowered or "income" in lowered:
                findings.append(
                    ReviewFinding(
                        kind="recurring_income_miss",
                        severity="high" if days_overdue > 7 else "medium",
                        summary=(
                            f"Income-like recurring pattern '{label}' appears delayed by "
                            f"{days_overdue} day(s)."
                        ),
                        evidence={
                            "label": label,
                            "expected_next_date": expected.isoformat(),
                            "days_overdue": days_overdue,
                            "confidence": item.get("confidence"),
                        },
                    )
                )

    mtd_kpis = month_to_date.get("kpis", {})
    mtd_income = _to_decimal(mtd_kpis.get("total_income"))
    mtd_expenses = _to_decimal(mtd_kpis.get("total_expenses"))
    mtd_net = _to_decimal(mtd_kpis.get("net_cashflow"))

    if mtd_income > Decimal("0.00") and mtd_expenses > mtd_income:
        findings.append(
            ReviewFinding(
                kind="mtd_cashflow_change",
                severity="medium",
                summary=(
                    f"Month-to-date expenses ({mtd_expenses:.2f}) exceed income ({mtd_income:.2f})."
                ),
                evidence={
                    "month_to_date_income": f"{mtd_income:.2f}",
                    "month_to_date_expenses": f"{mtd_expenses:.2f}",
                    "month_to_date_net_cashflow": f"{mtd_net:.2f}",
                },
            )
        )

    if goals and mtd_net < Decimal("0.00"):
        primary_goal = goals[0]
        findings.append(
            ReviewFinding(
                kind="goal_drift",
                severity="medium",
                summary=(
                    f"Negative month-to-date cashflow ({mtd_net:.2f}) may reduce progress "
                    f"toward goal '{primary_goal.title}'."
                ),
                evidence={
                    "goal_id": str(primary_goal.goal_id),
                    "goal_title": primary_goal.title,
                    "goal_status": primary_goal.status,
                    "month_to_date_net_cashflow": f"{mtd_net:.2f}",
                },
            )
        )

    findings.sort(key=lambda item: (_severity_rank(item.severity), item.kind, item.summary))
    return findings


def _to_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0.00")


def _normalize_severity(value: Any) -> ReviewSeverity:
    if not isinstance(value, str):
        return "medium"

    normalized = value.lower().strip()
    if normalized == "low":
        return "low"
    if normalized == "high":
        return "high"
    return "medium"


def _severity_rank(severity: str) -> int:
    mapping = {"high": 0, "medium": 1, "low": 2}
    return mapping.get(severity, 3)


def build_review_service(settings: Settings | None = None) -> ProactiveReviewService:
    cfg = settings or get_settings()
    memory_provider = OllamaEmbeddingProvider(
        base_url=str(cfg.ollama_base_url),
        model=cfg.memory_embedding_model,
        dimension=cfg.memory_embedding_dim,
        timeout_seconds=cfg.memory_embedding_timeout_seconds,
    )
    memory_service = SemanticMemoryService(
        embedding_provider=memory_provider,
        embedding_dimension=cfg.memory_embedding_dim,
    )
    return ProactiveReviewService(settings=cfg, memory_service=memory_service)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SentinelBudget proactive review backend")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run-review", help="Run and persist proactive review")
    run_parser.add_argument("--user-id", required=True, type=UUID)
    run_parser.add_argument("--mode", choices=["daily", "weekly"], default="daily")

    list_parser = subparsers.add_parser("list-insights", help="List recent insights")
    list_parser.add_argument("--user-id", required=True, type=UUID)
    list_parser.add_argument("--limit", type=int, default=50)

    unread_parser = subparsers.add_parser(
        "list-unread-insights",
        help="List unread insights",
    )
    unread_parser.add_argument("--user-id", required=True, type=UUID)
    unread_parser.add_argument("--limit", type=int, default=50)

    mark_parser = subparsers.add_parser("mark-insight-read", help="Mark one insight as read")
    mark_parser.add_argument("--user-id", required=True, type=UUID)
    mark_parser.add_argument("--insight-id", required=True, type=int)

    mark_many = subparsers.add_parser(
        "mark-insights-read",
        help="Mark multiple insights as read",
    )
    mark_many.add_argument("--user-id", required=True, type=UUID)
    mark_many.add_argument("--insight-id", action="append", required=True, type=int)

    daemon_parser = subparsers.add_parser("daemon", help="Run local scheduled review daemon")
    daemon_parser.add_argument("--user-id", action="append", required=True, type=UUID)

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    settings = get_settings()
    logger = setup_logging(settings.log_level)

    service = build_review_service(settings)

    user_id_obj = getattr(args, "user_id", None)
    user_context: str | list[str]
    if isinstance(user_id_obj, list):
        user_context = [str(item) for item in user_id_obj]
    elif user_id_obj is None:
        user_context = "n/a"
    else:
        user_context = str(user_id_obj)

    logger.info(
        "Review command started",
        extra={
            "command": "review",
            "subcommand": args.command,
            "user_id": user_context,
        },
    )

    try:
        if args.command == "daemon":
            from sentinelbudget.review.daemon import ReviewDaemon

            daemon = ReviewDaemon(settings=settings, review_service=service, user_ids=args.user_id)
            daemon.run_forever()
            return

        with transaction(settings) as conn:
            if args.command == "run-review":
                outcome = service.run_review(
                    conn,
                    user_id=args.user_id,
                    mode=args.mode,
                    persist=True,
                )
                print(json.dumps(outcome.to_dict(), indent=2, sort_keys=True))
                logger.info(
                    "Review run completed",
                    extra={
                        "command": "review",
                        "subcommand": "run-review",
                        "user_id": str(args.user_id),
                        "mode": args.mode,
                        "created_count": len(outcome.created_insight_ids),
                        "skipped_count": len(outcome.skipped_fingerprints),
                    },
                )
                return

            if args.command == "list-insights":
                insights = service.list_insights(conn, user_id=args.user_id, limit=args.limit)
                print(
                    json.dumps(
                        {"insights": [_insight_to_dict(item) for item in insights]},
                        indent=2,
                    )
                )
                logger.info(
                    "List insights completed",
                    extra={
                        "command": "review",
                        "subcommand": "list-insights",
                        "user_id": str(args.user_id),
                        "limit": args.limit,
                        "count": len(insights),
                    },
                )
                return

            if args.command == "list-unread-insights":
                insights = service.list_unread_insights(
                    conn,
                    user_id=args.user_id,
                    limit=args.limit,
                )
                print(
                    json.dumps(
                        {"insights": [_insight_to_dict(item) for item in insights]},
                        indent=2,
                    )
                )
                logger.info(
                    "List unread insights completed",
                    extra={
                        "command": "review",
                        "subcommand": "list-unread-insights",
                        "user_id": str(args.user_id),
                        "limit": args.limit,
                        "count": len(insights),
                    },
                )
                return

            if args.command == "mark-insight-read":
                updated = service.mark_insight_read(
                    conn,
                    user_id=args.user_id,
                    insight_id=args.insight_id,
                )
                print(json.dumps({"updated": updated}, indent=2))
                logger.info(
                    "Mark insight read completed",
                    extra={
                        "command": "review",
                        "subcommand": "mark-insight-read",
                        "user_id": str(args.user_id),
                        "insight_id": args.insight_id,
                        "updated": updated,
                    },
                )
                return

            updated_many = service.mark_insights_read(
                conn,
                user_id=args.user_id,
                insight_ids=args.insight_id,
            )
            print(json.dumps({"updated": updated_many}, indent=2))
            logger.info(
                "Mark insights read completed",
                extra={
                    "command": "review",
                    "subcommand": "mark-insights-read",
                    "user_id": str(args.user_id),
                    "requested_count": len(args.insight_id),
                    "updated_count": updated_many,
                },
            )
    except Exception as exc:  # pragma: no cover
        logger.error(
            "Review command failed",
            extra={
                "command": "review",
                "subcommand": args.command,
                "user_id": user_context,
                "detail": str(exc),
            },
        )
        raise SystemExit(1) from exc


def _insight_to_dict(insight: Insight) -> dict[str, Any]:
    return {
        "id": insight.id,
        "user_id": str(insight.user_id),
        "kind": insight.kind,
        "title": insight.title,
        "body": insight.body,
        "severity": insight.severity,
        "details": insight.details,
        "fingerprint": insight.fingerprint,
        "created_at": insight.created_at.isoformat(),
        "is_read": insight.is_read,
    }


if __name__ == "__main__":
    main()
