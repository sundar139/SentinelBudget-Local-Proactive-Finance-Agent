from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from psycopg import Connection


@dataclass(frozen=True, slots=True)
class Budget:
    budget_id: UUID
    user_id: UUID
    category_id: int | None
    period_month: date
    budget_amount: Decimal
    created_at: datetime


class BudgetRepository:
    """Repository for budgets table access."""

    @staticmethod
    def create(
        conn: Connection,
        user_id: UUID,
        period_month: date,
        budget_amount: Decimal,
        category_id: int | None = None,
        budget_id: UUID | None = None,
    ) -> Budget:
        resolved_budget_id = budget_id or uuid4()

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO budgets (budget_id, user_id, category_id, period_month, budget_amount)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING budget_id, user_id, category_id, period_month, budget_amount, created_at;
                """,
                (resolved_budget_id, user_id, category_id, period_month, budget_amount),
            )
            row = cur.fetchone()

        if row is None:  # pragma: no cover
            raise RuntimeError("Failed to insert budget")

        return Budget(
            budget_id=row[0],
            user_id=row[1],
            category_id=row[2],
            period_month=row[3],
            budget_amount=row[4],
            created_at=row[5],
        )

    @staticmethod
    def upsert(
        conn: Connection,
        user_id: UUID,
        period_month: date,
        budget_amount: Decimal,
        category_id: int | None = None,
        budget_id: UUID | None = None,
    ) -> Budget:
        resolved_budget_id = budget_id or uuid4()

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO budgets (budget_id, user_id, category_id, period_month, budget_amount)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (user_id, category_id, period_month)
                DO UPDATE SET budget_amount = EXCLUDED.budget_amount
                RETURNING budget_id, user_id, category_id, period_month, budget_amount, created_at;
                """,
                (resolved_budget_id, user_id, category_id, period_month, budget_amount),
            )
            row = cur.fetchone()

        if row is None:  # pragma: no cover
            raise RuntimeError("Failed to upsert budget")

        return Budget(
            budget_id=row[0],
            user_id=row[1],
            category_id=row[2],
            period_month=row[3],
            budget_amount=row[4],
            created_at=row[5],
        )

    @staticmethod
    def list_by_user(
        conn: Connection,
        user_id: UUID,
        period_month: date | None = None,
    ) -> list[Budget]:
        query = (
            """
            SELECT budget_id, user_id, category_id, period_month, budget_amount, created_at
            FROM budgets
            WHERE user_id = %s
            """
        )
        params: list[object] = [user_id]

        if period_month is not None:
            query += " AND period_month = %s"
            params.append(period_month)

        query += " ORDER BY period_month ASC, created_at ASC"

        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()

        return [
            Budget(
                budget_id=row[0],
                user_id=row[1],
                category_id=row[2],
                period_month=row[3],
                budget_amount=row[4],
                created_at=row[5],
            )
            for row in rows
        ]
