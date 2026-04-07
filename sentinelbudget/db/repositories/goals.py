from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from psycopg import Connection


@dataclass(frozen=True, slots=True)
class Goal:
    goal_id: UUID
    user_id: UUID
    title: str
    description: str | None
    target_amount: Decimal | None
    target_date: date | None
    status: str
    created_at: datetime


class GoalRepository:
    """Repository for goals table access."""

    @staticmethod
    def create(
        conn: Connection,
        user_id: UUID,
        title: str,
        description: str | None = None,
        target_amount: Decimal | None = None,
        target_date: date | None = None,
        status: str = "active",
        goal_id: UUID | None = None,
    ) -> Goal:
        resolved_goal_id = goal_id or uuid4()

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO goals (
                    goal_id,
                    user_id,
                    title,
                    description,
                    target_amount,
                    target_date,
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING
                    goal_id,
                    user_id,
                    title,
                    description,
                    target_amount,
                    target_date,
                    status,
                    created_at;
                """,
                (
                    resolved_goal_id,
                    user_id,
                    title,
                    description,
                    target_amount,
                    target_date,
                    status,
                ),
            )
            row = cur.fetchone()

        if row is None:  # pragma: no cover
            raise RuntimeError("Failed to insert goal")

        return Goal(
            goal_id=row[0],
            user_id=row[1],
            title=row[2],
            description=row[3],
            target_amount=row[4],
            target_date=row[5],
            status=row[6],
            created_at=row[7],
        )

    @staticmethod
    def list_by_user(conn: Connection, user_id: UUID, limit: int = 100) -> list[Goal]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    goal_id,
                    user_id,
                    title,
                    description,
                    target_amount,
                    target_date,
                    status,
                    created_at
                FROM goals
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT %s;
                """,
                (user_id, limit),
            )
            rows = cur.fetchall()

        return [
            Goal(
                goal_id=row[0],
                user_id=row[1],
                title=row[2],
                description=row[3],
                target_amount=row[4],
                target_date=row[5],
                status=row[6],
                created_at=row[7],
            )
            for row in rows
        ]
