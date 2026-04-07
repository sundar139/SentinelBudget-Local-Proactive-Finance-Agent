"""Phase 2 core relational schema.

Revision ID: 20260406_0001
Revises:
Create Date: 2026-04-06
"""

from __future__ import annotations

from alembic import op

revision = "20260406_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id UUID PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS accounts (
            account_id UUID PRIMARY KEY,
            user_id UUID NOT NULL REFERENCES users(user_id),
            institution TEXT NOT NULL,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            currency TEXT NOT NULL DEFAULT 'USD',
            current_balance NUMERIC(14,2) NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS categories (
            category_id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            parent_id INT NULL REFERENCES categories(category_id)
        );
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS ledger (
            trans_key BIGINT PRIMARY KEY,
            account_id UUID NOT NULL REFERENCES accounts(account_id),
            category_id INT NULL REFERENCES categories(category_id),
            ts TIMESTAMPTZ NOT NULL,
            amount NUMERIC(14,2) NOT NULL,
            currency TEXT NOT NULL DEFAULT 'USD',
            trans_type TEXT NOT NULL,
            description TEXT NULL,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb
        );
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS insights (
            id BIGSERIAL PRIMARY KEY,
            user_id UUID NOT NULL REFERENCES users(user_id),
            kind TEXT NOT NULL,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            severity TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            is_read BOOLEAN NOT NULL DEFAULT false
        );
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS goals (
            goal_id UUID PRIMARY KEY,
            user_id UUID NOT NULL REFERENCES users(user_id),
            title TEXT NOT NULL,
            description TEXT NULL,
            target_amount NUMERIC(14,2) NULL,
            target_date DATE NULL,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS budgets (
            budget_id UUID PRIMARY KEY,
            user_id UUID NOT NULL REFERENCES users(user_id),
            category_id INT NULL REFERENCES categories(category_id),
            period_month DATE NOT NULL,
            budget_amount NUMERIC(14,2) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_budgets_user_category_period UNIQUE (user_id, category_id, period_month)
        );
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS job_runs (
            job_run_id UUID PRIMARY KEY,
            job_name TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TIMESTAMPTZ NOT NULL,
            finished_at TIMESTAMPTZ NULL,
            details JSONB NOT NULL DEFAULT '{}'::jsonb
        );
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS user_preferences (
            preference_id UUID PRIMARY KEY,
            user_id UUID NOT NULL REFERENCES users(user_id),
            preference_key TEXT NOT NULL,
            preference_value JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_user_preferences_user_key UNIQUE (user_id, preference_key)
        );
        """
    )

    op.execute("CREATE INDEX IF NOT EXISTS idx_accounts_user_id ON accounts(user_id);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_ledger_account_ts ON ledger(account_id, ts);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_ledger_category_ts ON ledger(category_id, ts);")
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_insights_user_created_at "
        "ON insights(user_id, created_at DESC);"
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_goals_user_id ON goals(user_id);")
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_budgets_user_period "
        "ON budgets(user_id, period_month);"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_user_preferences_user_id ON user_preferences(user_id);"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_user_preferences_user_id;")
    op.execute("DROP INDEX IF EXISTS idx_budgets_user_period;")
    op.execute("DROP INDEX IF EXISTS idx_goals_user_id;")
    op.execute("DROP INDEX IF EXISTS idx_insights_user_created_at;")
    op.execute("DROP INDEX IF EXISTS idx_ledger_category_ts;")
    op.execute("DROP INDEX IF EXISTS idx_ledger_account_ts;")
    op.execute("DROP INDEX IF EXISTS idx_accounts_user_id;")

    op.execute("DROP TABLE IF EXISTS user_preferences CASCADE;")
    op.execute("DROP TABLE IF EXISTS job_runs CASCADE;")
    op.execute("DROP TABLE IF EXISTS budgets CASCADE;")
    op.execute("DROP TABLE IF EXISTS goals CASCADE;")
    op.execute("DROP TABLE IF EXISTS insights CASCADE;")
    op.execute("DROP TABLE IF EXISTS ledger CASCADE;")
    op.execute("DROP TABLE IF EXISTS categories CASCADE;")
    op.execute("DROP TABLE IF EXISTS accounts CASCADE;")
    op.execute("DROP TABLE IF EXISTS users CASCADE;")
