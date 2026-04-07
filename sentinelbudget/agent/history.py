from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from psycopg import Connection
from psycopg.types.json import Jsonb

from sentinelbudget.agent.models import ConversationEntry, ConversationRole

_ALLOWED_ROLES: tuple[ConversationRole, ...] = ("system", "user", "assistant", "tool")


class ConversationHistoryStore(Protocol):
    def append_message(
        self,
        conn: Connection,
        session_id: UUID,
        user_id: UUID,
        role: ConversationRole,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> ConversationEntry:
        raise NotImplementedError

    def list_recent(
        self,
        conn: Connection,
        session_id: UUID,
        user_id: UUID,
        limit: int = 40,
    ) -> list[ConversationEntry]:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class PostgresConversationHistoryStore:
    """PostgreSQL-backed persistent conversation history."""

    def append_message(
        self,
        conn: Connection,
        session_id: UUID,
        user_id: UUID,
        role: ConversationRole,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> ConversationEntry:
        if role not in _ALLOWED_ROLES:
            raise ValueError(f"role must be one of: {', '.join(_ALLOWED_ROLES)}")
        if content.strip() == "":
            raise ValueError("content cannot be empty")

        payload = {} if metadata is None else metadata
        if not isinstance(payload, dict):
            raise ValueError("metadata must be a JSON object")
        metadata_payload = Jsonb(payload)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO conversation_history (
                    session_id,
                    user_id,
                    role,
                    content,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s::jsonb)
                RETURNING id, session_id, user_id, role, content, metadata, created_at;
                """,
                (session_id, user_id, role, content, metadata_payload),
            )
            row = cur.fetchone()

        if row is None:  # pragma: no cover
            raise RuntimeError("Failed to insert conversation history row")

        return ConversationEntry(
            id=row[0],
            session_id=row[1],
            user_id=row[2],
            role=row[3],
            content=row[4],
            metadata=row[5],
            created_at=row[6],
        )

    def list_recent(
        self,
        conn: Connection,
        session_id: UUID,
        user_id: UUID,
        limit: int = 40,
    ) -> list[ConversationEntry]:
        if limit < 1:
            raise ValueError("limit must be positive")

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, session_id, user_id, role, content, metadata, created_at
                FROM conversation_history
                WHERE session_id = %s AND user_id = %s
                ORDER BY created_at DESC, id DESC
                LIMIT %s;
                """,
                (session_id, user_id, limit),
            )
            rows = cur.fetchall()

        reversed_rows = list(reversed(rows))
        return [
            ConversationEntry(
                id=row[0],
                session_id=row[1],
                user_id=row[2],
                role=row[3],
                content=row[4],
                metadata=row[5],
                created_at=row[6],
            )
            for row in reversed_rows
        ]
