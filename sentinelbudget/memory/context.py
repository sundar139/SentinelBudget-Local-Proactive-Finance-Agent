from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from psycopg import Connection

from sentinelbudget.db.repositories.goals import GoalRepository
from sentinelbudget.memory.models import ContextBundle, ContextGoal, MemoryKind
from sentinelbudget.memory.service import SemanticMemoryService


def assemble_context_bundle(
    conn: Connection,
    memory_service: SemanticMemoryService,
    user_id: UUID,
    query_text: str,
    top_k: int = 5,
    kind_filter: MemoryKind | None = None,
    analytics_summary: dict[str, Any] | None = None,
) -> ContextBundle:
    if top_k < 1:
        raise ValueError("top_k must be positive")

    memories = memory_service.query_memory(
        conn,
        user_id=user_id,
        query_text=query_text,
        kind=kind_filter,
        top_k=top_k,
    )

    goals = GoalRepository.list_by_user(conn, user_id, limit=10)
    goal_context = [
        ContextGoal(
            goal_id=goal.goal_id,
            title=goal.title,
            status=goal.status,
            target_amount=goal.target_amount,
            target_date=goal.target_date,
        )
        for goal in goals
    ]

    return ContextBundle(
        user_id=user_id,
        query_text=query_text,
        generated_at=datetime.now(UTC),
        top_k=top_k,
        memories=memories,
        goals=goal_context,
        analytics_summary=analytics_summary,
        meta={
            "memory_count": len(memories),
            "goal_count": len(goal_context),
            "kind_filter": kind_filter,
        },
    )
