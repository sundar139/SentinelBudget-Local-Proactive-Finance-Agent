from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from sentinelbudget.analytics.kpis import compute_kpis
from sentinelbudget.analytics.models import AnalyticsLedgerRow
from sentinelbudget.db.repositories.accounts import Account


def test_compute_kpis_math_and_savings_rate() -> None:
    account_id = uuid4()
    account = Account(
        account_id=account_id,
        user_id=uuid4(),
        institution="Test Bank",
        name="Checking",
        type="checking",
        currency="USD",
        current_balance=Decimal("1200.00"),
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    period_start = datetime(2026, 4, 1, tzinfo=UTC)
    period_end = datetime(2026, 4, 30, 23, 59, tzinfo=UTC)

    period_rows = [
        AnalyticsLedgerRow(
            trans_key=1,
            account_id=account_id,
            account_name="Checking",
            institution="Test Bank",
            category_id=1,
            category_name="Salary",
            ts=datetime(2026, 4, 1, 9, 0, tzinfo=UTC),
            amount=Decimal("5000.00"),
            currency="USD",
            trans_type="credit",
            description="Payroll",
            metadata={},
        ),
        AnalyticsLedgerRow(
            trans_key=2,
            account_id=account_id,
            account_name="Checking",
            institution="Test Bank",
            category_id=2,
            category_name="Rent",
            ts=datetime(2026, 4, 2, 8, 0, tzinfo=UTC),
            amount=Decimal("-1600.00"),
            currency="USD",
            trans_type="debit",
            description="Rent",
            metadata={},
        ),
        AnalyticsLedgerRow(
            trans_key=3,
            account_id=account_id,
            account_name="Checking",
            institution="Test Bank",
            category_id=3,
            category_name="Groceries",
            ts=datetime(2026, 4, 5, 18, 0, tzinfo=UTC),
            amount=Decimal("-300.00"),
            currency="USD",
            trans_type="debit",
            description="Groceries",
            metadata={},
        ),
    ]

    baseline_rows = period_rows + [
        AnalyticsLedgerRow(
            trans_key=4,
            account_id=account_id,
            account_name="Checking",
            institution="Test Bank",
            category_id=2,
            category_name="Rent",
            ts=datetime(2026, 3, 2, 8, 0, tzinfo=UTC),
            amount=Decimal("-1500.00"),
            currency="USD",
            trans_type="debit",
            description="Rent",
            metadata={},
        )
    ]

    result = compute_kpis(
        period_rows=period_rows,
        baseline_rows=baseline_rows,
        accounts=[account],
        period_start=period_start,
        period_end=period_end,
    )

    assert result.total_income == Decimal("5000.00")
    assert result.total_expenses == Decimal("1900.00")
    assert result.net_cashflow == Decimal("3100.00")
    assert result.average_daily_spend == Decimal("63.33")
    assert result.savings_rate == Decimal("0.6200")
    assert result.top_spending_categories[0].category_name == "Rent"
    assert result.account_balance_snapshots[0].projected_balance == Decimal("4300.00")
    assert result.month_over_month_spend is not None


def test_compute_kpis_month_over_month_none_without_prior_spend() -> None:
    account_id = uuid4()
    account = Account(
        account_id=account_id,
        user_id=uuid4(),
        institution="Test Bank",
        name="Checking",
        type="checking",
        currency="USD",
        current_balance=Decimal("100.00"),
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    period_rows = [
        AnalyticsLedgerRow(
            trans_key=10,
            account_id=account_id,
            account_name="Checking",
            institution="Test Bank",
            category_id=2,
            category_name="Rent",
            ts=datetime(2026, 4, 2, 8, 0, tzinfo=UTC),
            amount=Decimal("-1600.00"),
            currency="USD",
            trans_type="debit",
            description="Rent",
            metadata={},
        )
    ]

    result = compute_kpis(
        period_rows=period_rows,
        baseline_rows=period_rows,
        accounts=[account],
        period_start=datetime(2026, 4, 1, tzinfo=UTC),
        period_end=datetime(2026, 4, 30, 23, 59, tzinfo=UTC),
    )

    assert result.month_over_month_spend is None
