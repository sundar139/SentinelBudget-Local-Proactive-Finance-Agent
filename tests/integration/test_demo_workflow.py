from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import uuid4

import pytest
from psycopg import Connection
from sentinelbudget.config import get_settings
from sentinelbudget.db.repositories.ledger import LedgerRepository
from sentinelbudget.demo.bootstrap import bootstrap_demo_data


@pytest.mark.integration
def test_demo_bootstrap_data_is_idempotent_for_same_user_account(db_conn: Connection) -> None:
    settings = get_settings()
    user_id = uuid4()
    account_id = uuid4()

    first = bootstrap_demo_data(
        conn=db_conn,
        settings=settings,
        user_id=user_id,
        account_id=account_id,
        user_email=f"demo-{user_id}@example.com",
        institution="Demo Bank",
        account_name="Demo Checking",
        account_type="checking",
        starting_balance=Decimal("2500.00"),
        days=30,
        seed=42,
        start_date=date(2026, 1, 1),
        source_dataset="synthetic-demo",
        output_csv=None,
        sync_goals=False,
        review_mode="none",
    )

    second = bootstrap_demo_data(
        conn=db_conn,
        settings=settings,
        user_id=user_id,
        account_id=account_id,
        user_email=f"demo-{user_id}@example.com",
        institution="Demo Bank",
        account_name="Demo Checking",
        account_type="checking",
        starting_balance=Decimal("2500.00"),
        days=30,
        seed=42,
        start_date=date(2026, 1, 1),
        source_dataset="synthetic-demo",
        output_csv=None,
        sync_goals=False,
        review_mode="none",
    )

    assert first.ingest_summary["inserted_rows"] > 0
    ledger_rows = LedgerRepository.query_by_account(db_conn, account_id, limit=5000)
    assert len(ledger_rows) == first.ingest_summary["inserted_rows"]
    assert second.ingest_summary["inserted_rows"] == 0
    assert second.ingest_summary["duplicate_rows"] >= first.ingest_summary["inserted_rows"]
