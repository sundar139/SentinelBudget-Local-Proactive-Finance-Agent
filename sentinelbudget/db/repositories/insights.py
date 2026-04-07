from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from psycopg import Connection
from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb


@dataclass(frozen=True, slots=True)
class Insight:
    id: int
    user_id: UUID
    kind: str
    title: str
    body: str
    severity: str
    details: dict[str, Any]
    fingerprint: str | None
    created_at: datetime
    is_read: bool


class InsightRepository:
    """Repository for insights table access."""

    @staticmethod
    def create(
        conn: Connection,
        user_id: UUID,
        kind: str,
        title: str,
        body: str,
        severity: str,
        details: dict[str, Any] | None = None,
        fingerprint: str | None = None,
        is_read: bool = False,
    ) -> Insight:
        payload = {} if details is None else details
        if not isinstance(payload, dict):
            raise ValueError("details must be a JSON object")
        details_payload = Jsonb(payload)

        if fingerprint is not None and fingerprint.strip() == "":
            raise ValueError("fingerprint must be non-empty when provided")

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO insights (
                    user_id,
                    kind,
                    title,
                    body,
                    severity,
                    details,
                    fingerprint,
                    is_read
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                RETURNING
                    id,
                    user_id,
                    kind,
                    title,
                    body,
                    severity,
                    details,
                    fingerprint,
                    created_at,
                    is_read;
                """,
                (user_id, kind, title, body, severity, details_payload, fingerprint, is_read),
            )
            row = cur.fetchone()

        if row is None:  # pragma: no cover
            raise RuntimeError("Failed to insert insight")

        return Insight(
            id=row[0],
            user_id=row[1],
            kind=row[2],
            title=row[3],
            body=row[4],
            severity=row[5],
            details=row[6],
            fingerprint=row[7],
            created_at=row[8],
            is_read=row[9],
        )

    @staticmethod
    def create_if_new_unread(
        conn: Connection,
        user_id: UUID,
        kind: str,
        title: str,
        body: str,
        severity: str,
        details: dict[str, Any],
        fingerprint: str,
    ) -> tuple[Insight | None, bool]:
        if fingerprint.strip() == "":
            raise ValueError("fingerprint must be non-empty")

        existing = InsightRepository.get_unread_by_fingerprint(conn, user_id, fingerprint)
        if existing is not None:
            return existing, False

        try:
            created = InsightRepository.create(
                conn,
                user_id=user_id,
                kind=kind,
                title=title,
                body=body,
                severity=severity,
                details=details,
                fingerprint=fingerprint,
                is_read=False,
            )
        except UniqueViolation:
            existing_after_conflict = InsightRepository.get_unread_by_fingerprint(
                conn,
                user_id,
                fingerprint,
            )
            return existing_after_conflict, False

        return created, True

    @staticmethod
    def get_unread_by_fingerprint(
        conn: Connection,
        user_id: UUID,
        fingerprint: str,
    ) -> Insight | None:
        if fingerprint.strip() == "":
            raise ValueError("fingerprint must be non-empty")

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    user_id,
                    kind,
                    title,
                    body,
                    severity,
                    details,
                    fingerprint,
                    created_at,
                    is_read
                FROM insights
                WHERE user_id = %s
                  AND fingerprint = %s
                  AND is_read = false
                ORDER BY created_at DESC, id DESC
                LIMIT 1;
                """,
                (user_id, fingerprint),
            )
            row = cur.fetchone()

        if row is None:
            return None

        return Insight(
            id=row[0],
            user_id=row[1],
            kind=row[2],
            title=row[3],
            body=row[4],
            severity=row[5],
            details=row[6],
            fingerprint=row[7],
            created_at=row[8],
            is_read=row[9],
        )

    @staticmethod
    def list_by_user(
        conn: Connection,
        user_id: UUID,
        limit: int = 100,
        unread_only: bool = False,
    ) -> list[Insight]:
        if limit < 1:
            raise ValueError("limit must be positive")

        sql = (
            """
            SELECT
                id,
                user_id,
                kind,
                title,
                body,
                severity,
                details,
                fingerprint,
                created_at,
                is_read
            FROM insights
            WHERE user_id = %s
            """
        )
        params: list[Any] = [user_id]

        if unread_only:
            sql += " AND is_read = false"

        sql += " ORDER BY created_at DESC, id DESC LIMIT %s"
        params.append(limit)

        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

        return [
            Insight(
                id=row[0],
                user_id=row[1],
                kind=row[2],
                title=row[3],
                body=row[4],
                severity=row[5],
                details=row[6],
                fingerprint=row[7],
                created_at=row[8],
                is_read=row[9],
            )
            for row in rows
        ]

    @staticmethod
    def list_unread(conn: Connection, user_id: UUID, limit: int = 100) -> list[Insight]:
        return InsightRepository.list_by_user(conn, user_id=user_id, limit=limit, unread_only=True)

    @staticmethod
    def mark_read(conn: Connection, user_id: UUID, insight_id: int) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE insights
                SET is_read = true
                WHERE id = %s AND user_id = %s AND is_read = false;
                """,
                (insight_id, user_id),
            )
            updated = cur.rowcount

        return updated > 0

    @staticmethod
    def mark_many_read(conn: Connection, user_id: UUID, insight_ids: list[int]) -> int:
        if not insight_ids:
            return 0
        if any(item < 1 for item in insight_ids):
            raise ValueError("insight_ids must contain positive integers")

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE insights
                SET is_read = true
                WHERE user_id = %s
                  AND is_read = false
                  AND id = ANY(%s);
                """,
                (user_id, insight_ids),
            )
            updated = cur.rowcount

        return updated
