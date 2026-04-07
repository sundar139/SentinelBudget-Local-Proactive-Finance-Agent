from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from psycopg import Connection
from sentinelbudget.db.repositories import AccountRepository, UserRepository
from sentinelbudget.db.repositories.ledger import LedgerRepository
from sentinelbudget.db.schema import bootstrap_default_categories
from sentinelbudget.ingest.service import ingest_csv_file, ingest_synthetic_transactions

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def test_csv_ingest_quarantine_and_idempotency(db_conn: Connection) -> None:
    bootstrap_default_categories(db_conn)

    user = UserRepository.create(db_conn, email=f"ingest-{uuid4()}@example.com")
    account = AccountRepository.create(
        db_conn,
        user_id=user.user_id,
        institution="Test Bank",
        name="Ingest Account",
        account_type="checking",
        current_balance=Decimal("0.00"),
    )

    sample_path = FIXTURES_DIR / "finance_sample.csv"

    first = ingest_csv_file(
        conn=db_conn,
        dataset_type="finance",
        file_path=sample_path,
        account_id=account.account_id,
        source_dataset="finance-fixture",
        max_quarantine_ratio=0.8,
    )

    assert first.total_rows == 4
    assert first.inserted_rows == 3
    assert first.quarantined_rows == 1
    assert first.catastrophic_failure is False

    second = ingest_csv_file(
        conn=db_conn,
        dataset_type="finance",
        file_path=sample_path,
        account_id=account.account_id,
        source_dataset="finance-fixture",
        max_quarantine_ratio=0.8,
    )

    assert second.inserted_rows == 0
    assert second.duplicate_rows >= 3

    ledger_rows = LedgerRepository.query_by_account(db_conn, account.account_id, limit=1000)
    assert len(ledger_rows) == 3


def test_synthetic_ingest_loads_records(db_conn: Connection) -> None:
    bootstrap_default_categories(db_conn)

    user = UserRepository.create(db_conn, email=f"synthetic-{uuid4()}@example.com")
    account = AccountRepository.create(
        db_conn,
        user_id=user.user_id,
        institution="Test Bank",
        name="Synthetic Account",
        account_type="checking",
        current_balance=Decimal("0.00"),
    )

    summary = ingest_synthetic_transactions(
        conn=db_conn,
        account_id=account.account_id,
        days=90,
        seed=42,
        start_date=date(2026, 1, 1),
        source_dataset="synthetic-int",
        output_csv=None,
    )

    assert summary.total_rows >= 90
    assert summary.inserted_rows > 0

    second = ingest_synthetic_transactions(
        conn=db_conn,
        account_id=account.account_id,
        days=90,
        seed=42,
        start_date=date(2026, 1, 1),
        source_dataset="synthetic-int",
        output_csv=None,
    )

    assert second.inserted_rows == 0
    assert second.duplicate_rows == summary.total_rows

    ledger_rows = LedgerRepository.query_by_account(db_conn, account.account_id, limit=5000)
    assert len(ledger_rows) == summary.inserted_rows


def test_csv_ingest_catastrophic_quarantine_skips_insert(
    db_conn: Connection,
    tmp_path: Path,
) -> None:
    bootstrap_default_categories(db_conn)

    user = UserRepository.create(db_conn, email=f"catastrophic-{uuid4()}@example.com")
    account = AccountRepository.create(
        db_conn,
        user_id=user.user_id,
        institution="Test Bank",
        name="Catastrophic Account",
        account_type="checking",
        current_balance=Decimal("0.00"),
    )

    bad_csv = tmp_path / "mostly_bad.csv"
    bad_csv.write_text(
        "date,description,amount,category,institution,account,currency\n"
        "bad,Row 1,oops,Groceries,Bank,A,USD\n"
        "bad,Row 2,oops,Groceries,Bank,A,USD\n"
        "bad,Row 3,oops,Groceries,Bank,A,USD\n"
        "bad,Row 4,oops,Groceries,Bank,A,USD\n"
        "bad,Row 5,oops,Groceries,Bank,A,USD\n"
        "2026-01-01,Payroll,4500.00,Salary,Bank,A,USD\n",
        encoding="utf-8",
    )

    summary = ingest_csv_file(
        conn=db_conn,
        dataset_type="finance",
        file_path=bad_csv,
        account_id=account.account_id,
        source_dataset="catastrophic-fixture",
        max_quarantine_ratio=0.5,
    )

    assert summary.catastrophic_failure is True
    assert summary.inserted_rows == 0
    assert summary.quarantined_rows == 5

    ledger_rows = LedgerRepository.query_by_account(db_conn, account.account_id, limit=100)
    assert len(ledger_rows) == 0
