from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from psycopg import Connection
from sentinelbudget.config import get_settings
from sentinelbudget.db.repositories import AccountRepository, UserRepository
from sentinelbudget.db.schema import bootstrap_default_categories
from sentinelbudget.ingest.service import ingest_synthetic_transactions
from sentinelbudget.memory.embeddings import DummyDeterministicEmbeddingProvider
from sentinelbudget.memory.repository import SemanticMemoryRepository
from sentinelbudget.memory.service import SemanticMemoryService
from sentinelbudget.review.service import ProactiveReviewService


@pytest.mark.integration
def test_review_pipeline_persists_and_deduplicates(db_conn: Connection) -> None:
    bootstrap_default_categories(db_conn)

    user = UserRepository.create(db_conn, email=f"review-{uuid4()}@example.com")
    account = AccountRepository.create(
        db_conn,
        user_id=user.user_id,
        institution="Review Bank",
        name="Review Account",
        account_type="checking",
        current_balance=Decimal("2500.00"),
    )

    ingest_synthetic_transactions(
        conn=db_conn,
        account_id=account.account_id,
        days=120,
        seed=99,
        start_date=date(2026, 1, 1),
        source_dataset="review-int",
        output_csv=None,
    )

    dimension = SemanticMemoryRepository.get_embedding_dimension(db_conn)
    memory_service = SemanticMemoryService(
        DummyDeterministicEmbeddingProvider(dimension=dimension),
        embedding_dimension=dimension,
    )
    memory_service.store_memory(
        db_conn,
        user_id=user.user_id,
        kind="note",
        text="User wants to reduce restaurant spending",
        metadata={"source": "manual_note"},
    )

    service = ProactiveReviewService(
        settings=get_settings(),
        memory_service=memory_service,
    )

    now = datetime(2026, 4, 6, 12, 0, tzinfo=UTC)
    first = service.run_review(db_conn, user_id=user.user_id, mode="daily", reference_time=now)
    second = service.run_review(db_conn, user_id=user.user_id, mode="daily", reference_time=now)

    assert len(first.created_insight_ids) > 0
    assert len(second.created_insight_ids) == 0

    unread = service.list_unread_insights(db_conn, user_id=user.user_id, limit=100)
    assert len(unread) >= len(first.created_insight_ids)

    marked = service.mark_insight_read(db_conn, user_id=user.user_id, insight_id=unread[0].id)
    assert marked is True

    unread_after = service.list_unread_insights(db_conn, user_id=user.user_id, limit=100)
    assert len(unread_after) == len(unread) - 1

    weekly = service.run_review(db_conn, user_id=user.user_id, mode="weekly", reference_time=now)
    assert isinstance(weekly.created_insight_ids, list)

    unread_final = service.list_unread_insights(db_conn, user_id=user.user_id, limit=100)
    mark_ids = [item.id for item in unread_final[:2]]
    updated_many = service.mark_insights_read(db_conn, user_id=user.user_id, insight_ids=mark_ids)
    assert updated_many == len(mark_ids)
