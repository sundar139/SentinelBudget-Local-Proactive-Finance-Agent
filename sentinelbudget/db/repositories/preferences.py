from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from psycopg import Connection


@dataclass(frozen=True, slots=True)
class UserPreference:
    preference_id: UUID
    user_id: UUID
    preference_key: str
    preference_value: dict[str, Any]
    created_at: datetime


class UserPreferenceRepository:
    """Repository for user_preferences table access."""

    @staticmethod
    def create(
        conn: Connection,
        user_id: UUID,
        preference_key: str,
        preference_value: dict[str, Any],
        preference_id: UUID | None = None,
    ) -> UserPreference:
        resolved_preference_id = preference_id or uuid4()

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_preferences (
                    preference_id,
                    user_id,
                    preference_key,
                    preference_value
                )
                VALUES (%s, %s, %s, %s::jsonb)
                RETURNING preference_id, user_id, preference_key, preference_value, created_at;
                """,
                (resolved_preference_id, user_id, preference_key, preference_value),
            )
            row = cur.fetchone()

        if row is None:  # pragma: no cover
            raise RuntimeError("Failed to insert user preference")

        return UserPreference(
            preference_id=row[0],
            user_id=row[1],
            preference_key=row[2],
            preference_value=row[3],
            created_at=row[4],
        )

    @staticmethod
    def upsert(
        conn: Connection,
        user_id: UUID,
        preference_key: str,
        preference_value: dict[str, Any],
        preference_id: UUID | None = None,
    ) -> UserPreference:
        resolved_preference_id = preference_id or uuid4()

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_preferences (
                    preference_id,
                    user_id,
                    preference_key,
                    preference_value
                )
                VALUES (%s, %s, %s, %s::jsonb)
                ON CONFLICT (user_id, preference_key)
                DO UPDATE SET preference_value = EXCLUDED.preference_value
                RETURNING preference_id, user_id, preference_key, preference_value, created_at;
                """,
                (resolved_preference_id, user_id, preference_key, preference_value),
            )
            row = cur.fetchone()

        if row is None:  # pragma: no cover
            raise RuntimeError("Failed to upsert user preference")

        return UserPreference(
            preference_id=row[0],
            user_id=row[1],
            preference_key=row[2],
            preference_value=row[3],
            created_at=row[4],
        )

    @staticmethod
    def list_by_user(conn: Connection, user_id: UUID) -> list[UserPreference]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT preference_id, user_id, preference_key, preference_value, created_at
                FROM user_preferences
                WHERE user_id = %s
                ORDER BY preference_key ASC;
                """,
                (user_id,),
            )
            rows = cur.fetchall()

        return [
            UserPreference(
                preference_id=row[0],
                user_id=row[1],
                preference_key=row[2],
                preference_value=row[3],
                created_at=row[4],
            )
            for row in rows
        ]
