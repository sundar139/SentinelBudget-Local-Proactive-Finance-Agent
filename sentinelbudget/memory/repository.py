from __future__ import annotations

import math
import re
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, cast
from uuid import UUID

from psycopg import Connection
from psycopg.types.json import Jsonb

from sentinelbudget.memory.embeddings import validate_embedding_dimension
from sentinelbudget.memory.models import MemoryKind, SemanticMemoryItem, SemanticMemoryMatch


class SemanticMemoryRepository:
    """Repository for pgvector-backed semantic memory."""

    @staticmethod
    def get_embedding_dimension(conn: Connection) -> int:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT format_type(a.atttypid, a.atttypmod)
                FROM pg_attribute a
                INNER JOIN pg_class c ON c.oid = a.attrelid
                INNER JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relname = 'semantic_memory'
                  AND a.attname = 'embedding'
                  AND NOT a.attisdropped
                LIMIT 1;
                """
            )
            row = cur.fetchone()

        if row is None:
            raise RuntimeError("semantic_memory.embedding column not found")

        type_repr = str(row[0])
        match = re.search(r"vector\((\d+)\)", type_repr)
        if match is None:
            raise RuntimeError("Unable to determine semantic_memory embedding dimension")

        dimension = int(match.group(1))
        if dimension < 1:
            raise RuntimeError("Invalid semantic_memory embedding dimension")

        return dimension

    @staticmethod
    def ensure_embedding_dimension(conn: Connection, expected_dimension: int) -> None:
        actual_dimension = SemanticMemoryRepository.get_embedding_dimension(conn)
        if actual_dimension != expected_dimension:
            raise ValueError(
                "Configured embedding dimension does not match semantic_memory schema: "
                f"configured={expected_dimension}, schema={actual_dimension}"
            )

    @staticmethod
    def create(
        conn: Connection,
        user_id: UUID,
        kind: MemoryKind,
        text: str,
        embedding: list[float],
        metadata: dict[str, Any],
    ) -> SemanticMemoryItem:
        if text.strip() == "":
            raise ValueError("text cannot be empty")
        _validate_metadata(metadata)

        validate_embedding_dimension(
            embedding,
            SemanticMemoryRepository.get_embedding_dimension(conn),
        )
        vector_literal = _vector_literal(embedding)
        metadata_payload = Jsonb(metadata)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO semantic_memory (user_id, embedding, kind, text, metadata)
                VALUES (%s, %s::vector, %s, %s, %s::jsonb)
                RETURNING id, user_id, kind, text, metadata, created_at;
                """,
                (user_id, vector_literal, kind, text, metadata_payload),
            )
            row = cur.fetchone()

        if row is None:  # pragma: no cover
            raise RuntimeError("Failed to insert semantic memory")

        return SemanticMemoryItem(
            id=row[0],
            user_id=row[1],
            kind=cast(MemoryKind, row[2]),
            text=row[3],
            metadata=cast(dict[str, Any], row[4]),
            created_at=row[5],
        )

    @staticmethod
    def update(
        conn: Connection,
        memory_id: int,
        text: str,
        embedding: list[float],
        metadata: dict[str, Any],
    ) -> SemanticMemoryItem:
        if text.strip() == "":
            raise ValueError("text cannot be empty")
        _validate_metadata(metadata)

        validate_embedding_dimension(
            embedding,
            SemanticMemoryRepository.get_embedding_dimension(conn),
        )
        vector_literal = _vector_literal(embedding)
        metadata_payload = Jsonb(metadata)

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE semantic_memory
                SET embedding = %s::vector,
                    text = %s,
                    metadata = %s::jsonb
                WHERE id = %s
                RETURNING id, user_id, kind, text, metadata, created_at;
                """,
                (vector_literal, text, metadata_payload, memory_id),
            )
            row = cur.fetchone()

        if row is None:
            raise RuntimeError(f"semantic_memory item not found: {memory_id}")

        return SemanticMemoryItem(
            id=row[0],
            user_id=row[1],
            kind=cast(MemoryKind, row[2]),
            text=row[3],
            metadata=cast(dict[str, Any], row[4]),
            created_at=row[5],
        )

    @staticmethod
    def get_by_source(
        conn: Connection,
        user_id: UUID,
        kind: MemoryKind,
        source: str,
        source_id: str,
    ) -> SemanticMemoryItem | None:
        if source.strip() == "":
            raise ValueError("source cannot be empty")
        if source_id.strip() == "":
            raise ValueError("source_id cannot be empty")

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, user_id, kind, text, metadata, created_at
                FROM semantic_memory
                WHERE user_id = %s
                  AND kind = %s
                  AND metadata->>'source' = %s
                  AND metadata->>'source_id' = %s
                ORDER BY id DESC
                LIMIT 1;
                """,
                (user_id, kind, source, source_id),
            )
            row = cur.fetchone()

        if row is None:
            return None

        return SemanticMemoryItem(
            id=row[0],
            user_id=row[1],
            kind=cast(MemoryKind, row[2]),
            text=row[3],
            metadata=cast(dict[str, Any], row[4]),
            created_at=row[5],
        )

    @staticmethod
    def list_recent(
        conn: Connection,
        user_id: UUID,
        kind: MemoryKind | None = None,
        limit: int = 20,
    ) -> list[SemanticMemoryItem]:
        if limit < 1:
            raise ValueError("limit must be positive")

        sql = (
            """
            SELECT id, user_id, kind, text, metadata, created_at
            FROM semantic_memory
            WHERE user_id = %s
            """
        )
        params: list[Any] = [user_id]

        if kind is not None:
            sql += " AND kind = %s"
            params.append(kind)

        sql += " ORDER BY created_at DESC, id DESC LIMIT %s"
        params.append(limit)

        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

        return [
            SemanticMemoryItem(
                id=row[0],
                user_id=row[1],
                kind=cast(MemoryKind, row[2]),
                text=row[3],
                metadata=cast(dict[str, Any], row[4]),
                created_at=row[5],
            )
            for row in rows
        ]

    @staticmethod
    def search(
        conn: Connection,
        user_id: UUID,
        query_embedding: list[float],
        kind: MemoryKind | None = None,
        top_k: int = 5,
    ) -> list[SemanticMemoryMatch]:
        if top_k < 1:
            raise ValueError("top_k must be positive")

        validate_embedding_dimension(
            query_embedding,
            SemanticMemoryRepository.get_embedding_dimension(conn),
        )
        vector_literal = _vector_literal(query_embedding)

        sql = (
            """
            SELECT
                id,
                user_id,
                kind,
                text,
                metadata,
                created_at,
                (embedding <=> %s::vector) AS distance
            FROM semantic_memory
            WHERE user_id = %s
            """
        )
        params: list[Any] = [vector_literal, user_id]

        if kind is not None:
            sql += " AND kind = %s"
            params.append(kind)

        sql += " ORDER BY distance ASC, created_at DESC, id ASC LIMIT %s"
        params.append(top_k)

        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

        matches: list[SemanticMemoryMatch] = []
        for row in rows:
            score = _similarity_score_from_distance(row[6])
            matches.append(
                SemanticMemoryMatch(
                    id=row[0],
                    user_id=row[1],
                    kind=cast(MemoryKind, row[2]),
                    text=row[3],
                    metadata=cast(dict[str, Any], row[4]),
                    created_at=row[5],
                    score=score,
                )
            )

        return matches


def _vector_literal(embedding: list[float]) -> str:
    formatted_values: list[str] = []
    for value in embedding:
        if not math.isfinite(value):
            raise ValueError("Embedding contains non-finite values")
        formatted_values.append(f"{value:.8f}")

    return "[" + ",".join(formatted_values) + "]"


def _validate_metadata(metadata: dict[str, Any]) -> None:
    if not isinstance(metadata, dict):
        raise ValueError("metadata must be a JSON object")


def _similarity_score_from_distance(distance_value: Any) -> Decimal:
    distance = Decimal(str(distance_value))
    if not distance.is_finite():
        raise RuntimeError("semantic memory query returned non-finite distance")

    similarity = Decimal("1.0") - distance
    if similarity < Decimal("-1.0"):
        similarity = Decimal("-1.0")
    elif similarity > Decimal("1.0"):
        similarity = Decimal("1.0")

    return similarity.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
