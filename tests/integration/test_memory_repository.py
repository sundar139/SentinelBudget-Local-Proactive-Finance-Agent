from __future__ import annotations

from datetime import UTC
from decimal import Decimal
from uuid import uuid4

import pytest
from psycopg import Connection
from sentinelbudget.config import get_settings
from sentinelbudget.db.repositories.goals import GoalRepository
from sentinelbudget.db.repositories.users import UserRepository
from sentinelbudget.memory.context import assemble_context_bundle
from sentinelbudget.memory.embeddings import (
    DummyDeterministicEmbeddingProvider,
    OllamaEmbeddingProvider,
)
from sentinelbudget.memory.repository import SemanticMemoryRepository
from sentinelbudget.memory.service import SemanticMemoryService


def _build_service(db_conn: Connection) -> SemanticMemoryService:
    dimension = SemanticMemoryRepository.get_embedding_dimension(db_conn)
    provider = DummyDeterministicEmbeddingProvider(dimension=dimension)
    return SemanticMemoryService(provider, embedding_dimension=dimension)


def test_semantic_memory_dimension_matches_configuration(db_conn: Connection) -> None:
    configured_dimension = get_settings().memory_embedding_dim
    schema_dimension = SemanticMemoryRepository.get_embedding_dimension(db_conn)

    assert schema_dimension == configured_dimension


def test_ui_memory_runtime_path_uses_consistent_dimension(db_conn: Connection) -> None:
    settings = get_settings()
    user = UserRepository.create(db_conn, email=f"memory-ui-{uuid4()}@example.com")

    # Mirrors ui/state.py wiring: SemanticMemoryService with OllamaEmbeddingProvider.
    service = SemanticMemoryService(
        embedding_provider=OllamaEmbeddingProvider(
            base_url=str(settings.ollama_base_url),
            model=settings.memory_embedding_model,
            dimension=settings.memory_embedding_dim,
            timeout_seconds=settings.memory_embedding_timeout_seconds,
        ),
        embedding_dimension=settings.memory_embedding_dim,
    )

    items = service.list_memory(db_conn, user_id=user.user_id, limit=5)

    assert items == []


def test_pgvector_store_and_similarity_query(db_conn: Connection) -> None:
    user = UserRepository.create(db_conn, email=f"memory-{uuid4()}@example.com")
    service = _build_service(db_conn)

    service.store_memory(
        db_conn,
        user_id=user.user_id,
        kind="note",
        text="Emergency fund target is 10000",
        metadata={"source": "manual_note"},
    )
    service.store_memory(
        db_conn,
        user_id=user.user_id,
        kind="note",
        text="Vacation budget should stay low",
        metadata={"source": "manual_note"},
    )
    service.store_memory(
        db_conn,
        user_id=user.user_id,
        kind="preference",
        text="User prefers conservative spending alerts",
        metadata={"source": "user_preferences"},
    )

    first_query = service.query_memory(
        db_conn,
        user_id=user.user_id,
        query_text="Emergency fund target is 10000",
        top_k=3,
    )
    second_query = service.query_memory(
        db_conn,
        user_id=user.user_id,
        query_text="Emergency fund target is 10000",
        top_k=3,
    )

    assert first_query[0].text == "Emergency fund target is 10000"
    assert [item.id for item in first_query] == [item.id for item in second_query]

    preference_only = service.query_memory(
        db_conn,
        user_id=user.user_id,
        query_text="alerts",
        kind="preference",
        top_k=3,
    )
    assert preference_only
    assert all(item.kind == "preference" for item in preference_only)


def test_goal_sync_is_duplicate_safe(db_conn: Connection) -> None:
    user = UserRepository.create(db_conn, email=f"goal-sync-{uuid4()}@example.com")
    service = _build_service(db_conn)

    goal = GoalRepository.create(
        db_conn,
        user_id=user.user_id,
        title="Emergency Fund",
        description="Save for emergencies",
        target_amount=Decimal("8000.00"),
        status="active",
    )

    first = service.sync_goals(db_conn, user.user_id)
    second = service.sync_goals(db_conn, user.user_id)

    assert first.inserted == 1
    assert second.skipped == 1

    with db_conn.cursor() as cur:
        cur.execute(
            "UPDATE goals SET title = %s WHERE goal_id = %s;",
            ("Emergency Reserve", goal.goal_id),
        )

    third = service.sync_goals(db_conn, user.user_id)
    assert third.updated == 1

    goal_memories = service.list_memory(db_conn, user.user_id, kind="goal", limit=20)
    assert len(goal_memories) == 1
    assert "Emergency Reserve" in goal_memories[0].text


def test_context_assembly_structure(db_conn: Connection) -> None:
    user = UserRepository.create(db_conn, email=f"context-{uuid4()}@example.com")
    service = _build_service(db_conn)

    GoalRepository.create(
        db_conn,
        user_id=user.user_id,
        title="Retirement",
        description="Invest monthly",
        target_amount=Decimal("500000.00"),
        status="active",
    )
    service.store_memory(
        db_conn,
        user_id=user.user_id,
        kind="note",
        text="User wants to reduce discretionary spend",
        metadata={"source": "manual_note", "created_at": str(UTC)},
    )

    bundle = assemble_context_bundle(
        db_conn,
        memory_service=service,
        user_id=user.user_id,
        query_text="am I overspending?",
        top_k=5,
        analytics_summary={"net_cashflow": "1200.00"},
    )

    payload = bundle.to_dict()
    assert payload["query_text"] == "am I overspending?"
    assert payload["meta"]["memory_count"] >= 1
    assert payload["meta"]["goal_count"] >= 1
    assert "analytics_summary" in payload


def test_query_memory_rejects_invalid_inputs(db_conn: Connection) -> None:
    user = UserRepository.create(db_conn, email=f"guard-{uuid4()}@example.com")
    service = _build_service(db_conn)

    with pytest.raises(ValueError, match="top_k must be positive"):
        service.query_memory(
            db_conn,
            user_id=user.user_id,
            query_text="budget",
            top_k=0,
        )

    with pytest.raises(ValueError, match="query_text cannot be empty"):
        service.query_memory(
            db_conn,
            user_id=user.user_id,
            query_text="   ",
            top_k=5,
        )


def test_store_memory_rejects_non_object_metadata(db_conn: Connection) -> None:
    user = UserRepository.create(db_conn, email=f"metadata-{uuid4()}@example.com")
    service = _build_service(db_conn)

    with pytest.raises(ValueError, match="metadata must be a JSON object"):
        service.store_memory(
            db_conn,
            user_id=user.user_id,
            kind="note",
            text="Note with invalid metadata",
            metadata=["not", "an", "object"],  # type: ignore[arg-type]
        )
