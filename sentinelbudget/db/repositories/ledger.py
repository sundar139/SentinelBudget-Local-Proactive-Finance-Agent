from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from psycopg import Connection
from psycopg.types.json import Jsonb


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    trans_key: int
    account_id: UUID
    category_id: int | None
    ts: datetime
    amount: Decimal
    currency: str
    trans_type: str
    description: str | None
    metadata: dict[str, Any]


@dataclass(frozen=True, slots=True)
class LedgerEntryWithContext:
    trans_key: int
    account_id: UUID
    account_name: str
    institution: str
    category_id: int | None
    category_name: str | None
    ts: datetime
    amount: Decimal
    currency: str
    trans_type: str
    description: str | None
    metadata: dict[str, Any]


class LedgerRepository:
    """Repository for ledger table access."""

    @staticmethod
    def exists_by_natural_key(
        conn: Connection,
        account_id: UUID,
        ts: datetime,
        amount: Decimal,
        description: str | None,
    ) -> bool:
        normalized_description = (description or "").strip().lower()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS(
                    SELECT 1
                    FROM ledger
                    WHERE account_id = %s
                      AND ts = %s
                      AND amount = %s
                      AND lower(btrim(COALESCE(description, ''))) = %s
                );
                """,
                (account_id, ts, amount, normalized_description),
            )
            row = cur.fetchone()

        return bool(row and row[0])

    @staticmethod
    def insert_if_absent(
        conn: Connection,
        trans_key: int,
        account_id: UUID,
        ts: datetime,
        amount: Decimal,
        trans_type: str,
        category_id: int | None = None,
        currency: str = "USD",
        description: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        metadata_payload = Jsonb(metadata or {})

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ledger (
                    trans_key,
                    account_id,
                    category_id,
                    ts,
                    amount,
                    currency,
                    trans_type,
                    description,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (trans_key)
                DO NOTHING
                RETURNING trans_key;
                """,
                (
                    trans_key,
                    account_id,
                    category_id,
                    ts,
                    amount,
                    currency,
                    trans_type,
                    description,
                    metadata_payload,
                ),
            )
            row = cur.fetchone()

        return row is not None

    @staticmethod
    def insert(
        conn: Connection,
        trans_key: int,
        account_id: UUID,
        ts: datetime,
        amount: Decimal,
        trans_type: str,
        category_id: int | None = None,
        currency: str = "USD",
        description: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> LedgerEntry:
        metadata_payload = Jsonb(metadata or {})

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ledger (
                    trans_key,
                    account_id,
                    category_id,
                    ts,
                    amount,
                    currency,
                    trans_type,
                    description,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                RETURNING
                    trans_key,
                    account_id,
                    category_id,
                    ts,
                    amount,
                    currency,
                    trans_type,
                    description,
                    metadata;
                """,
                (
                    trans_key,
                    account_id,
                    category_id,
                    ts,
                    amount,
                    currency,
                    trans_type,
                    description,
                    metadata_payload,
                ),
            )
            row = cur.fetchone()

        if row is None:  # pragma: no cover
            raise RuntimeError("Failed to insert ledger entry")

        return LedgerEntry(
            trans_key=row[0],
            account_id=row[1],
            category_id=row[2],
            ts=row[3],
            amount=row[4],
            currency=row[5],
            trans_type=row[6],
            description=row[7],
            metadata=row[8],
        )

    @staticmethod
    def query_by_account(
        conn: Connection,
        account_id: UUID,
        start_ts: datetime | None = None,
        end_ts: datetime | None = None,
        limit: int = 500,
    ) -> list[LedgerEntry]:
        sql = (
            """
            SELECT
                trans_key,
                account_id,
                category_id,
                ts,
                amount,
                currency,
                trans_type,
                description,
                metadata
            FROM ledger
            WHERE account_id = %s
            """
        )
        params: list[Any] = [account_id]

        if start_ts is not None:
            sql += " AND ts >= %s"
            params.append(start_ts)

        if end_ts is not None:
            sql += " AND ts <= %s"
            params.append(end_ts)

        sql += " ORDER BY ts DESC, trans_key DESC LIMIT %s"
        params.append(limit)

        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

        return [
            LedgerEntry(
                trans_key=row[0],
                account_id=row[1],
                category_id=row[2],
                ts=row[3],
                amount=row[4],
                currency=row[5],
                trans_type=row[6],
                description=row[7],
                metadata=row[8],
            )
            for row in rows
        ]

    @staticmethod
    def query_for_user(
        conn: Connection,
        user_id: UUID,
        start_ts: datetime | None = None,
        end_ts: datetime | None = None,
        account_ids: list[UUID] | None = None,
    ) -> list[LedgerEntryWithContext]:
        sql = (
            """
            SELECT
                l.trans_key,
                l.account_id,
                a.name,
                a.institution,
                l.category_id,
                c.name,
                l.ts,
                l.amount,
                l.currency,
                l.trans_type,
                l.description,
                l.metadata
            FROM ledger l
            INNER JOIN accounts a
                ON a.account_id = l.account_id
            LEFT JOIN categories c
                ON c.category_id = l.category_id
            WHERE a.user_id = %s
            """
        )
        params: list[Any] = [user_id]

        if start_ts is not None:
            sql += " AND l.ts >= %s"
            params.append(start_ts)

        if end_ts is not None:
            sql += " AND l.ts <= %s"
            params.append(end_ts)

        if account_ids:
            sql += " AND l.account_id = ANY(%s)"
            params.append(account_ids)

        sql += " ORDER BY l.ts ASC, l.trans_key ASC"

        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

        return [
            LedgerEntryWithContext(
                trans_key=row[0],
                account_id=row[1],
                account_name=row[2],
                institution=row[3],
                category_id=row[4],
                category_name=row[5],
                ts=row[6],
                amount=row[7],
                currency=row[8],
                trans_type=row[9],
                description=row[10],
                metadata=row[11],
            )
            for row in rows
        ]
