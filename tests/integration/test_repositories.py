from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import psycopg.errors
import pytest
from psycopg import Connection
from sentinelbudget.db.repositories import (
    AccountRepository,
    BudgetRepository,
    CategoryRepository,
    GoalRepository,
    InsightRepository,
    LedgerRepository,
    UserPreferenceRepository,
    UserRepository,
)


def _test_email() -> str:
    return f"phase2-{uuid4()}@example.com"


def _test_bigint_key() -> int:
    return int(uuid4().int % 9_000_000_000_000_000_000)


def test_basic_repository_roundtrip(db_conn: Connection) -> None:
    user = UserRepository.create(db_conn, email=_test_email())
    loaded_user = UserRepository.get_by_id(db_conn, user.user_id)
    assert loaded_user is not None
    assert loaded_user.email == user.email

    account = AccountRepository.create(
        db_conn,
        user_id=user.user_id,
        institution="Bank A",
        name="Checking",
        account_type="checking",
        current_balance=Decimal("100.50"),
    )
    loaded_account = AccountRepository.get_by_id(db_conn, account.account_id)
    assert loaded_account is not None
    assert loaded_account.name == "Checking"

    parent = CategoryRepository.create(db_conn, name=f"Test Parent {uuid4()}")
    child = CategoryRepository.create(
        db_conn,
        name=f"Test Child {uuid4()}",
        parent_id=parent.category_id,
    )
    all_categories = CategoryRepository.list_all(db_conn)
    assert any(item.category_id == parent.category_id for item in all_categories)
    assert any(item.category_id == child.category_id for item in all_categories)

    entry = LedgerRepository.insert(
        db_conn,
        trans_key=_test_bigint_key(),
        account_id=account.account_id,
        category_id=child.category_id,
        ts=datetime.now(UTC),
        amount=Decimal("12.34"),
        trans_type="debit",
        description="Coffee",
        metadata={"merchant": "Cafe"},
    )
    entries = LedgerRepository.query_by_account(db_conn, account.account_id)
    assert any(item.trans_key == entry.trans_key for item in entries)

    insight = InsightRepository.create(
        db_conn,
        user_id=user.user_id,
        kind="notice",
        title="Spending Check",
        body="Spending increased this week.",
        severity="medium",
    )
    insights = InsightRepository.list_by_user(db_conn, user.user_id)
    assert any(item.id == insight.id for item in insights)

    goal = GoalRepository.create(
        db_conn,
        user_id=user.user_id,
        title="Emergency Fund",
        target_amount=Decimal("1000.00"),
        target_date=date(2026, 12, 31),
    )
    goals = GoalRepository.list_by_user(db_conn, user.user_id)
    assert any(item.goal_id == goal.goal_id for item in goals)

    budget = BudgetRepository.create(
        db_conn,
        user_id=user.user_id,
        category_id=child.category_id,
        period_month=date(2026, 4, 1),
        budget_amount=Decimal("500.00"),
    )
    budgets = BudgetRepository.list_by_user(db_conn, user.user_id)
    assert any(item.budget_id == budget.budget_id for item in budgets)

    preference = UserPreferenceRepository.create(
        db_conn,
        user_id=user.user_id,
        preference_key="notifications",
        preference_value={"enabled": True},
    )
    loaded_preferences = UserPreferenceRepository.list_by_user(db_conn, user.user_id)
    assert any(item.preference_id == preference.preference_id for item in loaded_preferences)


def test_uniqueness_constraints_enforced(db_conn: Connection) -> None:
    email = _test_email()
    UserRepository.create(db_conn, email=email)

    with pytest.raises(psycopg.errors.UniqueViolation):
        UserRepository.create(db_conn, email=email)


def test_budget_upsert_updates_existing_row(db_conn: Connection) -> None:
    user = UserRepository.create(db_conn, email=_test_email())
    category = CategoryRepository.create(db_conn, name=f"Budget Category {uuid4()}")
    period_month = date(2026, 4, 1)

    first = BudgetRepository.upsert(
        db_conn,
        user_id=user.user_id,
        category_id=category.category_id,
        period_month=period_month,
        budget_amount=Decimal("300.00"),
        budget_id=UUID("11111111-1111-1111-1111-111111111111"),
    )
    second = BudgetRepository.upsert(
        db_conn,
        user_id=user.user_id,
        category_id=category.category_id,
        period_month=period_month,
        budget_amount=Decimal("450.00"),
        budget_id=UUID("22222222-2222-2222-2222-222222222222"),
    )

    assert first.budget_id == second.budget_id
    assert second.budget_amount == Decimal("450.00")

    budgets = BudgetRepository.list_by_user(db_conn, user.user_id, period_month=period_month)
    assert len(budgets) == 1
    assert budgets[0].budget_amount == Decimal("450.00")


def test_category_hierarchy_behavior(db_conn: Connection) -> None:
    parent = CategoryRepository.create(db_conn, name=f"Parent {uuid4()}")
    child = CategoryRepository.create(
        db_conn,
        name=f"Child {uuid4()}",
        parent_id=parent.category_id,
    )

    categories = CategoryRepository.list_all(db_conn)
    category_map = {item.category_id: item for item in categories}

    assert parent.category_id in category_map
    assert child.category_id in category_map
    assert category_map[child.category_id].parent_id == parent.category_id
