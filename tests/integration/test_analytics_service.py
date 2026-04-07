from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

from psycopg import Connection
from sentinelbudget.analytics.service import run_analytics
from sentinelbudget.db.repositories import AccountRepository, UserRepository
from sentinelbudget.db.schema import bootstrap_default_categories
from sentinelbudget.ingest.service import ingest_synthetic_transactions


def test_analytics_service_returns_structured_result(db_conn: Connection) -> None:
    bootstrap_default_categories(db_conn)

    user = UserRepository.create(db_conn, email=f"analytics-{uuid4()}@example.com")
    account = AccountRepository.create(
        db_conn,
        user_id=user.user_id,
        institution="Test Bank",
        name="Analytics Account",
        account_type="checking",
        current_balance=Decimal("500.00"),
    )

    ingest_synthetic_transactions(
        conn=db_conn,
        account_id=account.account_id,
        days=120,
        seed=7,
        start_date=date(2026, 1, 1),
        source_dataset="analytics-int",
        output_csv=None,
    )

    result = run_analytics(
        conn=db_conn,
        user_id=user.user_id,
        window="last_30_days",
        reference_time=datetime(2026, 4, 30, 23, 59, tzinfo=UTC),
    )

    assert result.user_id == user.user_id
    assert result.kpis.total_income >= Decimal("0.00")
    assert result.kpis.total_expenses >= Decimal("0.00")
    assert result.kpis.net_cashflow == result.kpis.total_income - result.kpis.total_expenses
    assert len(result.kpis.account_balance_snapshots) == 1
    assert result.meta["period_rows"] > 0

    payload = result.to_dict()
    assert payload["time_window"]["label"] == "last_30_days"
    assert "kpis" in payload
    assert "recurring_candidates" in payload
    assert "anomaly_events" in payload
