from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import uuid4

import pytest
from psycopg import Connection
from sentinelbudget.config import get_settings
from sentinelbudget.db.repositories.goals import GoalRepository
from sentinelbudget.db.repositories.ledger import LedgerRepository
from sentinelbudget.demo.bootstrap import bootstrap_demo_data
from sentinelbudget.memory.repository import SemanticMemoryRepository


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

    goals_after_first = GoalRepository.list_by_user(db_conn, user_id=user_id, limit=20)
    goal_titles = {item.title for item in goals_after_first}
    assert {
        "Build Emergency Fund",
        "Reduce Dining-Out Spending",
        "Save for Travel",
    }.issubset(goal_titles)

    assert second.ingest_summary["inserted_rows"] == 0
    assert second.ingest_summary["duplicate_rows"] >= first.ingest_summary["inserted_rows"]

    goals_after_second = GoalRepository.list_by_user(db_conn, user_id=user_id, limit=20)
    assert len(goals_after_second) == len(goals_after_first)


@pytest.mark.integration
def test_demo_bootstrap_sync_goals_populates_semantic_memory(db_conn: Connection) -> None:
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
        sync_goals=True,
        review_mode="none",
    )

    assert first.synced_goals is not None
    assert first.synced_goals["processed"] >= 3
    assert first.synced_goals["inserted"] >= 3

    first_goal_memory = SemanticMemoryRepository.list_recent(
        db_conn,
        user_id=user_id,
        kind="goal",
        limit=50,
    )
    assert len(first_goal_memory) >= 3

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
        sync_goals=True,
        review_mode="none",
    )

    assert second.synced_goals is not None
    assert second.synced_goals["inserted"] == 0
    assert second.synced_goals["skipped"] >= 3

    second_goal_memory = SemanticMemoryRepository.list_recent(
        db_conn,
        user_id=user_id,
        kind="goal",
        limit=50,
    )
    assert len(second_goal_memory) == len(first_goal_memory)
