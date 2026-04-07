from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from psycopg import Connection


@dataclass(frozen=True, slots=True)
class Account:
    account_id: UUID
    user_id: UUID
    institution: str
    name: str
    type: str
    currency: str
    current_balance: Decimal
    created_at: datetime


class AccountRepository:
    """Repository for accounts table access."""

    @staticmethod
    def create(
        conn: Connection,
        user_id: UUID,
        institution: str,
        name: str,
        account_type: str,
        currency: str = "USD",
        current_balance: Decimal = Decimal("0.00"),
        account_id: UUID | None = None,
    ) -> Account:
        resolved_account_id = account_id or uuid4()

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO accounts (
                    account_id,
                    user_id,
                    institution,
                    name,
                    type,
                    currency,
                    current_balance
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING
                    account_id,
                    user_id,
                    institution,
                    name,
                    type,
                    currency,
                    current_balance,
                    created_at;
                """,
                (
                    resolved_account_id,
                    user_id,
                    institution,
                    name,
                    account_type,
                    currency,
                    current_balance,
                ),
            )
            row = cur.fetchone()

        if row is None:  # pragma: no cover
            raise RuntimeError("Failed to insert account")

        return Account(
            account_id=row[0],
            user_id=row[1],
            institution=row[2],
            name=row[3],
            type=row[4],
            currency=row[5],
            current_balance=row[6],
            created_at=row[7],
        )

    @staticmethod
    def get_by_id(conn: Connection, account_id: UUID) -> Account | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    account_id,
                    user_id,
                    institution,
                    name,
                    type,
                    currency,
                    current_balance,
                    created_at
                FROM accounts
                WHERE account_id = %s;
                """,
                (account_id,),
            )
            row = cur.fetchone()

        if row is None:
            return None

        return Account(
            account_id=row[0],
            user_id=row[1],
            institution=row[2],
            name=row[3],
            type=row[4],
            currency=row[5],
            current_balance=row[6],
            created_at=row[7],
        )

    @staticmethod
    def list_by_user(conn: Connection, user_id: UUID) -> list[Account]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    account_id,
                    user_id,
                    institution,
                    name,
                    type,
                    currency,
                    current_balance,
                    created_at
                FROM accounts
                WHERE user_id = %s
                ORDER BY created_at ASC;
                """,
                (user_id,),
            )
            rows = cur.fetchall()

        return [
            Account(
                account_id=row[0],
                user_id=row[1],
                institution=row[2],
                name=row[3],
                type=row[4],
                currency=row[5],
                current_balance=row[6],
                created_at=row[7],
            )
            for row in rows
        ]
