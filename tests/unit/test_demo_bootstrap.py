from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sentinelbudget.db.repositories.accounts import Account
from sentinelbudget.db.repositories.users import User
from sentinelbudget.demo.bootstrap import (
    _ensure_user,
    bootstrap_demo_data,
    run_demo_bootstrap,
)
from sentinelbudget.ingest.models import IngestSummary


def _fake_ingest_summary() -> IngestSummary:
    return IngestSummary(
        dataset_type="synthetic",
        source_dataset="synthetic-demo",
        total_rows=10,
        inserted_rows=9,
        duplicate_rows=1,
        quarantined_rows=0,
        normalized_rows=10,
        catastrophic_failure=False,
    )


def test_run_demo_bootstrap_runs_migrations_then_bootstrap(monkeypatch) -> None:
    fake_settings = SimpleNamespace(log_level="INFO")
    fake_conn = object()
    called: dict[str, object] = {}

    @contextmanager
    def _fake_transaction(settings):
        assert settings is fake_settings
        yield fake_conn

    def _fake_run_migrations(revision: str) -> None:
        called["revision"] = revision

    def _fake_bootstrap(**kwargs):
        called["conn"] = kwargs["conn"]
        return SimpleNamespace(to_dict=lambda: {}, user_id=uuid4(), account_id=uuid4())

    monkeypatch.setattr("sentinelbudget.demo.bootstrap.run_migrations", _fake_run_migrations)
    monkeypatch.setattr("sentinelbudget.demo.bootstrap.transaction", _fake_transaction)
    monkeypatch.setattr("sentinelbudget.demo.bootstrap.bootstrap_demo_data", _fake_bootstrap)

    run_demo_bootstrap(
        settings=fake_settings,
        user_id=uuid4(),
        account_id=uuid4(),
        user_email="demo@example.com",
        institution="Bank",
        account_name="Checking",
        account_type="checking",
        starting_balance=Decimal("1000.00"),
        days=30,
        seed=42,
        start_date=date(2026, 1, 1),
        source_dataset="synthetic-demo",
        output_csv=None,
        sync_goals=False,
        review_mode="none",
    )

    assert called["revision"] == "head"
    assert called["conn"] is fake_conn


def test_bootstrap_demo_data_shapes_result(monkeypatch) -> None:
    user_id = uuid4()
    account_id = uuid4()
    now = datetime(2026, 4, 6, 12, 0, tzinfo=UTC)

    fake_user = User(user_id=user_id, email="demo@example.com", created_at=now)
    fake_account = Account(
        account_id=account_id,
        user_id=user_id,
        institution="Bank",
        name="Checking",
        type="checking",
        currency="USD",
        current_balance=Decimal("1000.00"),
        created_at=now,
    )

    monkeypatch.setattr(
        "sentinelbudget.demo.bootstrap.bootstrap_default_categories",
        lambda conn: 5,
    )
    monkeypatch.setattr(
        "sentinelbudget.demo.bootstrap._ensure_user",
        lambda conn, user_id, user_email: (fake_user, True, ["email mismatch warning"]),
    )

    def _fake_ensure_account(
        conn,
        account_id,
        user_id,
        institution,
        account_name,
        account_type,
        starting_balance,
    ):
        del conn, account_id, user_id, institution, account_name, account_type, starting_balance
        return fake_account, False

    monkeypatch.setattr(
        "sentinelbudget.demo.bootstrap._ensure_account",
        _fake_ensure_account,
    )

    def _fake_ingest_synthetic_transactions(
        conn,
        account_id,
        days,
        seed,
        start_date,
        source_dataset,
        output_csv,
    ):
        del conn, account_id, days, seed, start_date, source_dataset, output_csv
        return _fake_ingest_summary()

    monkeypatch.setattr(
        "sentinelbudget.demo.bootstrap.ingest_synthetic_transactions",
        _fake_ingest_synthetic_transactions,
    )
    monkeypatch.setattr(
        "sentinelbudget.demo.bootstrap._sync_goals",
        lambda conn, settings, user_id, sync_goals: None,
    )
    monkeypatch.setattr(
        "sentinelbudget.demo.bootstrap._run_review",
        lambda conn, settings, user_id, review_mode: (2, 1, "daily"),
    )

    result = bootstrap_demo_data(
        conn=object(),
        settings=SimpleNamespace(),
        user_id=user_id,
        account_id=account_id,
        user_email="demo@example.com",
        institution="Bank",
        account_name="Checking",
        account_type="checking",
        starting_balance=Decimal("1000.00"),
        days=30,
        seed=42,
        start_date=date(2026, 1, 1),
        source_dataset="synthetic-demo",
        output_csv=None,
        sync_goals=False,
        review_mode="daily",
    )

    assert result.user_created is True
    assert result.account_created is False
    assert result.categories_inserted == 5
    assert result.ingest_summary["inserted_rows"] == 9
    assert result.review_created_count == 2
    assert result.review_skipped_count == 1
    assert result.warnings == ["email mismatch warning"]
    assert any("streamlit run ui/app.py" in item for item in result.next_commands)
    assert any("sentinelbudget-chat ask" in item for item in result.next_commands)


def test_ensure_user_warns_when_email_differs(monkeypatch) -> None:
    existing_user_id = UUID("00000000-0000-0000-0000-000000000101")
    now = datetime(2026, 4, 6, 12, 0, tzinfo=UTC)
    existing = User(user_id=existing_user_id, email="existing@example.com", created_at=now)

    monkeypatch.setattr(
        "sentinelbudget.demo.bootstrap.UserRepository.get_by_id",
        lambda conn, user_id: existing,
    )

    user, created, warnings = _ensure_user(
        conn=object(),
        user_id=existing_user_id,
        user_email="new@example.com",
    )

    assert created is False
    assert user.email == "existing@example.com"
    assert warnings


def test_bootstrap_demo_data_rejects_invalid_user_email() -> None:
    user_id = uuid4()
    account_id = uuid4()

    with pytest.raises(ValueError, match="user_email"):
        bootstrap_demo_data(
            conn=object(),
            settings=SimpleNamespace(),
            user_id=user_id,
            account_id=account_id,
            user_email="not-an-email",
            institution="Bank",
            account_name="Checking",
            account_type="checking",
            starting_balance=Decimal("1000.00"),
            days=30,
            seed=42,
            start_date=date(2026, 1, 1),
            source_dataset="synthetic-demo",
            output_csv=None,
            sync_goals=False,
            review_mode="none",
        )


def test_bootstrap_demo_data_rejects_blank_source_dataset() -> None:
    user_id = uuid4()
    account_id = uuid4()

    with pytest.raises(ValueError, match="source_dataset"):
        bootstrap_demo_data(
            conn=object(),
            settings=SimpleNamespace(),
            user_id=user_id,
            account_id=account_id,
            user_email="demo@example.com",
            institution="Bank",
            account_name="Checking",
            account_type="checking",
            starting_balance=Decimal("1000.00"),
            days=30,
            seed=42,
            start_date=date(2026, 1, 1),
            source_dataset="  ",
            output_csv=None,
            sync_goals=False,
            review_mode="none",
        )


def test_bootstrap_demo_data_rejects_output_csv_directory(tmp_path) -> None:
    user_id = uuid4()
    account_id = uuid4()

    with pytest.raises(ValueError, match="output_csv"):
        bootstrap_demo_data(
            conn=object(),
            settings=SimpleNamespace(),
            user_id=user_id,
            account_id=account_id,
            user_email="demo@example.com",
            institution="Bank",
            account_name="Checking",
            account_type="checking",
            starting_balance=Decimal("1000.00"),
            days=30,
            seed=42,
            start_date=date(2026, 1, 1),
            source_dataset="synthetic-demo",
            output_csv=tmp_path,
            sync_goals=False,
            review_mode="none",
        )


def test_bootstrap_demo_data_surfaces_ingest_failure_context(monkeypatch) -> None:
    user_id = uuid4()
    account_id = uuid4()
    now = datetime(2026, 4, 6, 12, 0, tzinfo=UTC)

    fake_user = User(user_id=user_id, email="demo@example.com", created_at=now)
    fake_account = Account(
        account_id=account_id,
        user_id=user_id,
        institution="Bank",
        name="Checking",
        type="checking",
        currency="USD",
        current_balance=Decimal("1000.00"),
        created_at=now,
    )

    def _fake_bootstrap_default_categories(conn):
        del conn
        return 5

    monkeypatch.setattr(
        "sentinelbudget.demo.bootstrap.bootstrap_default_categories",
        _fake_bootstrap_default_categories,
    )
    monkeypatch.setattr(
        "sentinelbudget.demo.bootstrap._ensure_user",
        lambda conn, user_id, user_email: (fake_user, False, []),
    )
    def _fake_ensure_account(
        conn,
        account_id,
        user_id,
        institution,
        account_name,
        account_type,
        starting_balance,
    ):
        del conn, account_id, user_id, institution, account_name, account_type, starting_balance
        return fake_account, False

    monkeypatch.setattr("sentinelbudget.demo.bootstrap._ensure_account", _fake_ensure_account)

    def _fail_ingest(**kwargs):
        del kwargs
        raise ValueError("cannot adapt type 'dict' using placeholder '%s' (format: AUTO)")

    monkeypatch.setattr("sentinelbudget.demo.bootstrap.ingest_synthetic_transactions", _fail_ingest)

    with pytest.raises(RuntimeError, match="Synthetic ingest failed during demo bootstrap") as exc:
        bootstrap_demo_data(
            conn=object(),
            settings=SimpleNamespace(),
            user_id=user_id,
            account_id=account_id,
            user_email="demo@example.com",
            institution="Bank",
            account_name="Checking",
            account_type="checking",
            starting_balance=Decimal("1000.00"),
            days=30,
            seed=42,
            start_date=date(2026, 1, 1),
            source_dataset="synthetic-demo",
            output_csv=None,
            sync_goals=False,
            review_mode="none",
        )

    message = str(exc.value)
    assert "account_id" in message
    assert "source_dataset" in message
    assert "days=30" in message
    assert "seed=42" in message
