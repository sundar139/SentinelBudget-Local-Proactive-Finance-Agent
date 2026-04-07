from __future__ import annotations

from psycopg import Connection

EXPECTED_TABLES = {
    "users",
    "accounts",
    "categories",
    "ledger",
    "insights",
    "goals",
    "budgets",
    "job_runs",
    "user_preferences",
}

EXPECTED_INDEXES = {
    "idx_accounts_user_id",
    "idx_ledger_account_ts",
    "idx_ledger_category_ts",
    "idx_insights_user_created_at",
    "idx_goals_user_id",
    "idx_budgets_user_period",
    "idx_user_preferences_user_id",
}


def test_expected_tables_exist(db_conn: Connection) -> None:
    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename = ANY(%s);
            """,
            (list(EXPECTED_TABLES),),
        )
        rows = cur.fetchall()

    existing = {row[0] for row in rows}
    assert EXPECTED_TABLES.issubset(existing)


def test_expected_indexes_exist(db_conn: Connection) -> None:
    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND indexname = ANY(%s);
            """,
            (list(EXPECTED_INDEXES),),
        )
        rows = cur.fetchall()

    existing = {row[0] for row in rows}
    assert EXPECTED_INDEXES.issubset(existing)
