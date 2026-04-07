from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import UUID

from psycopg import Connection

from sentinelbudget.analytics.anomalies import detect_anomalies
from sentinelbudget.analytics.cashflow import resolve_time_window
from sentinelbudget.analytics.kpis import compute_kpis
from sentinelbudget.analytics.models import AnalyticsLedgerRow, AnalyticsRunResult
from sentinelbudget.analytics.recurring import detect_recurring_candidates
from sentinelbudget.config import get_settings
from sentinelbudget.db.repositories.accounts import AccountRepository
from sentinelbudget.db.repositories.ledger import LedgerEntryWithContext, LedgerRepository
from sentinelbudget.db.repositories.session import transaction
from sentinelbudget.logging import setup_logging


def _to_analytics_rows(rows: list[LedgerEntryWithContext]) -> list[AnalyticsLedgerRow]:
    return [
        AnalyticsLedgerRow(
            trans_key=row.trans_key,
            account_id=row.account_id,
            account_name=row.account_name,
            institution=row.institution,
            category_id=row.category_id,
            category_name=row.category_name,
            ts=row.ts,
            amount=row.amount,
            currency=row.currency,
            trans_type=row.trans_type,
            description=row.description,
            metadata=row.metadata,
        )
        for row in rows
    ]


def _validate_window_args(
    window: str,
    custom_start: date | None,
    custom_end: date | None,
) -> None:
    if window != "custom" and (custom_start is not None or custom_end is not None):
        raise ValueError("custom_start/custom_end can only be used when window=custom")


def _validate_currency_consistency(rows: list[AnalyticsLedgerRow]) -> None:
    currencies = sorted({row.currency for row in rows})
    if len(currencies) > 1:
        raise ValueError(
            "Analytics currently supports a single currency per run. "
            f"Found currencies: {', '.join(currencies)}"
        )


def run_analytics(
    conn: Connection,
    user_id: UUID,
    window: str,
    account_filter_ids: list[UUID] | None = None,
    custom_start: date | None = None,
    custom_end: date | None = None,
    reference_time: datetime | None = None,
) -> AnalyticsRunResult:
    _validate_window_args(window, custom_start, custom_end)

    now = reference_time.astimezone(UTC) if reference_time else datetime.now(UTC)
    time_window = resolve_time_window(window, now, custom_start=custom_start, custom_end=custom_end)

    baseline_start = time_window.start_ts - timedelta(days=180)
    account_ids = account_filter_ids or []

    baseline_repo_rows = LedgerRepository.query_for_user(
        conn,
        user_id=user_id,
        start_ts=baseline_start,
        end_ts=time_window.end_ts,
        account_ids=account_ids,
    )
    baseline_rows = _to_analytics_rows(baseline_repo_rows)
    _validate_currency_consistency(baseline_rows)

    period_rows = [
        row
        for row in baseline_rows
        if time_window.start_ts <= row.ts <= time_window.end_ts
    ]

    accounts = AccountRepository.list_by_user(conn, user_id)
    if account_ids:
        allowed = {str(item) for item in account_ids}
        accounts = [account for account in accounts if str(account.account_id) in allowed]

    kpis = compute_kpis(
        period_rows=period_rows,
        baseline_rows=baseline_rows,
        accounts=accounts,
        period_start=time_window.start_ts,
        period_end=time_window.end_ts,
    )
    recurring = detect_recurring_candidates(baseline_rows)
    anomalies = detect_anomalies(
        baseline_rows=baseline_rows,
        window_start=time_window.start_ts,
        window_end=time_window.end_ts,
    )

    return AnalyticsRunResult(
        user_id=user_id,
        generated_at=now,
        time_window=time_window,
        account_filter_ids=account_ids,
        kpis=kpis,
        recurring_candidates=recurring,
        anomaly_events=anomalies,
        meta={
            "baseline_rows": len(baseline_rows),
            "period_rows": len(period_rows),
        },
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SentinelBudget deterministic analytics")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common_args(target: argparse.ArgumentParser) -> None:
        target.add_argument("--user-id", required=True, type=UUID)
        target.add_argument(
            "--window",
            choices=["last_7_days", "last_30_days", "month_to_date", "custom"],
            default="last_30_days",
        )
        target.add_argument("--custom-start", type=date.fromisoformat, default=None)
        target.add_argument("--custom-end", type=date.fromisoformat, default=None)
        target.add_argument("--account-id", action="append", type=UUID, default=[])

    summary_parser = subparsers.add_parser("summary", help="Run KPI and category summary analytics")
    add_common_args(summary_parser)

    recurring_parser = subparsers.add_parser(
        "recurring",
        help="Run recurring spend/income detection",
    )
    add_common_args(recurring_parser)

    anomalies_parser = subparsers.add_parser("anomalies", help="Run anomaly scan")
    add_common_args(anomalies_parser)

    all_parser = subparsers.add_parser("all", help="Run full deterministic analytics suite")
    add_common_args(all_parser)

    return parser


def _select_output(command: str, result: AnalyticsRunResult) -> dict[str, Any]:
    payload = result.to_dict()

    if command == "summary":
        return {
            "user_id": payload["user_id"],
            "generated_at": payload["generated_at"],
            "time_window": payload["time_window"],
            "kpis": payload["kpis"],
            "meta": payload["meta"],
        }

    if command == "recurring":
        return {
            "user_id": payload["user_id"],
            "generated_at": payload["generated_at"],
            "time_window": payload["time_window"],
            "recurring_candidates": payload["recurring_candidates"],
            "meta": payload["meta"],
        }

    if command == "anomalies":
        return {
            "user_id": payload["user_id"],
            "generated_at": payload["generated_at"],
            "time_window": payload["time_window"],
            "anomaly_events": payload["anomaly_events"],
            "meta": payload["meta"],
        }

    return payload


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    settings = get_settings()
    logger = setup_logging(settings.log_level)

    logger.info(
        "Analytics command started",
        extra={
            "command": "analytics",
            "subcommand": args.command,
            "user_id": str(args.user_id),
            "window": args.window,
            "account_count": len(args.account_id),
        },
    )

    try:
        with transaction(settings) as conn:
            result = run_analytics(
                conn=conn,
                user_id=args.user_id,
                window=args.window,
                account_filter_ids=args.account_id,
                custom_start=args.custom_start,
                custom_end=args.custom_end,
            )

        print(json.dumps(_select_output(args.command, result), indent=2, sort_keys=True))
        logger.info(
            "Analytics command completed",
            extra={
                "command": "analytics",
                "subcommand": args.command,
                "user_id": str(args.user_id),
                "window": args.window,
                "period_rows": result.meta.get("period_rows", 0),
                "baseline_rows": result.meta.get("baseline_rows", 0),
            },
        )
    except Exception as exc:  # pragma: no cover
        logger.error(
            "Analytics run failed",
            extra={
                "command": "analytics",
                "subcommand": args.command,
                "user_id": str(args.user_id),
                "window": args.window,
                "detail": str(exc),
            },
        )
        sys.exit(1)
