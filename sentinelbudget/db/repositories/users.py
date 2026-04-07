from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid4

from psycopg import Connection


@dataclass(frozen=True, slots=True)
class User:
    user_id: UUID
    email: str
    created_at: datetime


class UserRepository:
    """Repository for users table access."""

    @staticmethod
    def create(conn: Connection, email: str, user_id: UUID | None = None) -> User:
        resolved_user_id = user_id or uuid4()

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (user_id, email)
                VALUES (%s, %s)
                RETURNING user_id, email, created_at;
                """,
                (resolved_user_id, email),
            )
            row = cur.fetchone()

        if row is None:  # pragma: no cover
            raise RuntimeError("Failed to insert user")

        return User(user_id=row[0], email=row[1], created_at=row[2])

    @staticmethod
    def get_by_id(conn: Connection, user_id: UUID) -> User | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_id, email, created_at
                FROM users
                WHERE user_id = %s;
                """,
                (user_id,),
            )
            row = cur.fetchone()

        if row is None:
            return None

        return User(user_id=row[0], email=row[1], created_at=row[2])

    @staticmethod
    def get_by_email(conn: Connection, email: str) -> User | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_id, email, created_at
                FROM users
                WHERE email = %s;
                """,
                (email,),
            )
            row = cur.fetchone()

        if row is None:
            return None

        return User(user_id=row[0], email=row[1], created_at=row[2])
