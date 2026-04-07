from __future__ import annotations

import csv
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from io import StringIO
from typing import Any

from sentinelbudget.db.repositories.ledger import LedgerEntryWithContext


@dataclass(frozen=True, slots=True)
class MonthlyCashflowPoint:
    month: str
    income: Decimal
    expenses: Decimal
    net: Decimal


@dataclass(frozen=True, slots=True)
class TransactionRecord:
    trans_key: int
    posted_at: datetime
    account_name: str
    institution: str
    category_name: str
    description: str
    direction: str
    amount: Decimal
    currency: str
    is_anomaly: bool


def has_valid_custom_date_range(custom_start: date | None, custom_end: date | None) -> bool:
    if custom_start is None or custom_end is None:
        return True
    return custom_start <= custom_end


def _round_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def build_monthly_cashflow_points(
    rows: Sequence[LedgerEntryWithContext],
) -> list[MonthlyCashflowPoint]:
    grouped: dict[str, tuple[Decimal, Decimal]] = {}

    for row in rows:
        month_key = row.ts.strftime("%Y-%m")
        income, expenses = grouped.get(month_key, (Decimal("0.00"), Decimal("0.00")))

        if row.amount >= Decimal("0.00"):
            income += row.amount
        else:
            expenses += abs(row.amount)

        grouped[month_key] = (income, expenses)

    points: list[MonthlyCashflowPoint] = []
    for month_key in sorted(grouped.keys()):
        income, expenses = grouped[month_key]
        points.append(
            MonthlyCashflowPoint(
                month=month_key,
                income=_round_money(income),
                expenses=_round_money(expenses),
                net=_round_money(income - expenses),
            )
        )

    return points


def category_chart_rows(top_categories: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    parsed: list[dict[str, Any]] = []

    for item in top_categories:
        category_name = str(item.get("category_name", "Uncategorized"))
        spend = Decimal(str(item.get("total_spend", "0")))
        parsed.append({"category": category_name, "spend": float(spend)})

    parsed.sort(key=lambda entry: (-entry["spend"], str(entry["category"]).lower()))
    return parsed


def count_anomalies_by_severity(anomaly_events: Iterable[dict[str, Any]]) -> dict[str, int]:
    counts = {"high": 0, "medium": 0, "low": 0, "unknown": 0}

    for item in anomaly_events:
        severity = str(item.get("severity", "unknown")).lower()
        if severity in counts:
            counts[severity] += 1
        else:
            counts["unknown"] += 1

    return counts


def build_transaction_records(
    rows: Sequence[LedgerEntryWithContext],
    anomaly_trans_keys: set[int] | None = None,
) -> list[TransactionRecord]:
    anomaly_keys = anomaly_trans_keys or set()
    out: list[TransactionRecord] = []

    for row in rows:
        category_name = row.category_name or "Uncategorized"
        direction = "inflow" if row.amount >= Decimal("0.00") else "outflow"
        out.append(
            TransactionRecord(
                trans_key=row.trans_key,
                posted_at=row.ts,
                account_name=row.account_name,
                institution=row.institution,
                category_name=category_name,
                description=(row.description or "").strip(),
                direction=direction,
                amount=_round_money(row.amount),
                currency=row.currency,
                is_anomaly=row.trans_key in anomaly_keys,
            )
        )

    out.sort(key=lambda item: (item.posted_at, item.trans_key), reverse=True)
    return out


def filter_transaction_records(
    records: Sequence[TransactionRecord],
    account_names: set[str],
    categories: set[str],
    directions: set[str],
    search_text: str,
    anomalies_only: bool,
) -> list[TransactionRecord]:
    lowered_search = search_text.strip().lower()
    filtered: list[TransactionRecord] = []

    for record in records:
        if account_names and record.account_name not in account_names:
            continue
        if categories and record.category_name not in categories:
            continue
        if directions and record.direction not in directions:
            continue
        if anomalies_only and not record.is_anomaly:
            continue

        if lowered_search:
            haystack = " ".join(
                [
                    record.account_name,
                    record.institution,
                    record.category_name,
                    record.description,
                    str(record.trans_key),
                ]
            ).lower()
            if lowered_search not in haystack:
                continue

        filtered.append(record)

    return filtered


def transaction_records_to_rows(records: Sequence[TransactionRecord]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in records:
        out.append(
            {
                "timestamp": item.posted_at.isoformat(),
                "trans_key": item.trans_key,
                "account": item.account_name,
                "institution": item.institution,
                "category": item.category_name,
                "direction": item.direction,
                "amount": f"{item.amount:.2f}",
                "currency": item.currency,
                "description": item.description,
                "is_anomaly": item.is_anomaly,
            }
        )
    return out


def records_to_csv(records: Sequence[dict[str, Any]]) -> str:
    if not records:
        return ""

    columns = list(records[0].keys())
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns)
    writer.writeheader()
    for row in records:
        writer.writerow(row)

    return buffer.getvalue()
