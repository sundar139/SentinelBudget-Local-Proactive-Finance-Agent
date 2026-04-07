from __future__ import annotations

from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal

from sentinelbudget.analytics.cashflow import category_spend_summaries, quantize_money
from sentinelbudget.analytics.models import (
    AccountBalanceSnapshot,
    AnalyticsLedgerRow,
    KpiSummary,
    MonthOverMonthSpendComparison,
)
from sentinelbudget.db.repositories.accounts import Account


def _q(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def compute_kpis(
    period_rows: list[AnalyticsLedgerRow],
    baseline_rows: list[AnalyticsLedgerRow],
    accounts: list[Account],
    period_start: datetime,
    period_end: datetime,
) -> KpiSummary:
    income = _q(
        sum(
            (row.amount for row in period_rows if row.amount > Decimal("0.00")),
            Decimal("0.00"),
        )
    )
    expenses = _q(
        sum(
            (abs(row.amount) for row in period_rows if row.amount < Decimal("0.00")),
            Decimal("0.00"),
        )
    )
    net_cashflow = _q(income - expenses)

    total_days = max(1, (period_end.date() - period_start.date()).days + 1)
    avg_daily_spend = _q(expenses / Decimal(total_days))

    savings_rate: Decimal | None
    if income > Decimal("0.00"):
        savings_rate = (net_cashflow / income).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    else:
        savings_rate = None

    spending_by_category = category_spend_summaries(
        [(row.category_id, row.category_name, row.amount) for row in period_rows]
    )
    top_spending = spending_by_category[:5]

    mom = _month_over_month_spend(baseline_rows, period_start, period_end)
    account_snapshots = _account_snapshots(accounts, period_rows)

    return KpiSummary(
        total_income=income,
        total_expenses=expenses,
        net_cashflow=net_cashflow,
        average_daily_spend=avg_daily_spend,
        savings_rate=savings_rate,
        spending_by_category=spending_by_category,
        top_spending_categories=top_spending,
        month_over_month_spend=mom,
        account_balance_snapshots=account_snapshots,
    )


def _account_snapshots(
    accounts: list[Account],
    period_rows: list[AnalyticsLedgerRow],
) -> list[AccountBalanceSnapshot]:
    by_account: dict[str, list[AnalyticsLedgerRow]] = {}
    for row in period_rows:
        by_account.setdefault(str(row.account_id), []).append(row)

    snapshots: list[AccountBalanceSnapshot] = []
    for account in accounts:
        rows = by_account.get(str(account.account_id), [])
        income = _q(
            sum(
                (row.amount for row in rows if row.amount > Decimal("0.00")),
                Decimal("0.00"),
            )
        )
        expenses = _q(
            sum(
                (abs(row.amount) for row in rows if row.amount < Decimal("0.00")),
                Decimal("0.00"),
            )
        )
        net = _q(income - expenses)
        projected = quantize_money(account.current_balance + net)

        snapshots.append(
            AccountBalanceSnapshot(
                account_id=account.account_id,
                account_name=account.name,
                institution=account.institution,
                current_balance=quantize_money(account.current_balance),
                period_income=income,
                period_expenses=expenses,
                period_net_cashflow=net,
                projected_balance=projected,
            )
        )

    snapshots.sort(key=lambda item: (item.institution.lower(), item.account_name.lower()))
    return snapshots


def _month_over_month_spend(
    baseline_rows: list[AnalyticsLedgerRow],
    period_start: datetime,
    period_end: datetime,
) -> MonthOverMonthSpendComparison | None:
    period_days = (period_end.date() - period_start.date()).days + 1
    if period_days <= 0:
        return None

    previous_start = period_start - timedelta(days=period_days)
    previous_end = period_start - timedelta(microseconds=1)

    current_expense_rows = [
        row for row in baseline_rows if period_start <= row.ts <= period_end and row.amount < 0
    ]
    previous_expense_rows = [
        row
        for row in baseline_rows
        if previous_start <= row.ts <= previous_end and row.amount < 0
    ]

    if not previous_expense_rows:
        return None

    current_spend = _q(
        sum(
            (abs(row.amount) for row in current_expense_rows),
            Decimal("0.00"),
        )
    )
    previous_spend = _q(
        sum(
            (abs(row.amount) for row in previous_expense_rows),
            Decimal("0.00"),
        )
    )

    if previous_spend == Decimal("0.00"):
        delta_percent: Decimal | None = None
    else:
        delta_percent = ((current_spend - previous_spend) / previous_spend).quantize(
            Decimal("0.0001"),
            rounding=ROUND_HALF_UP,
        )

    return MonthOverMonthSpendComparison(
        current_period_spend=current_spend,
        previous_period_spend=previous_spend,
        delta_amount=_q(current_spend - previous_spend),
        delta_percent=delta_percent,
    )
