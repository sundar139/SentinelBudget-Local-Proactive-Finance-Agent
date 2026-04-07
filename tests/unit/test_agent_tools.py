from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest
from psycopg import Connection
from sentinelbudget.agent.tools import AgentToolRegistry, ToolValidationError, UnsupportedToolError
from sentinelbudget.analytics.models import AnalyticsRunResult, KpiSummary, TimeWindow
from sentinelbudget.memory.models import SemanticMemoryMatch


class _FakeMemoryService:
    def query_memory(
        self,
        conn: Connection,
        user_id: object,
        query_text: str,
        kind: str | None,
        top_k: int,
    ) -> list[SemanticMemoryMatch]:
        del conn, user_id, query_text, kind, top_k
        return [
            SemanticMemoryMatch(
                id=1,
                user_id=uuid4(),
                kind="note",
                text="Dining spend is trending high",
                metadata={"source": "manual_note"},
                created_at=datetime(2026, 4, 6, tzinfo=UTC),
                score=Decimal("0.9000"),
            )
        ]


def _fake_analytics_runner(**kwargs: object) -> AnalyticsRunResult:
    user_id = cast(object, kwargs["user_id"])
    return AnalyticsRunResult(
        user_id=cast(Any, user_id),
        generated_at=datetime(2026, 4, 6, tzinfo=UTC),
        time_window=TimeWindow(
            label="last_30_days",
            start_ts=datetime(2026, 3, 7, tzinfo=UTC),
            end_ts=datetime(2026, 4, 6, tzinfo=UTC),
        ),
        account_filter_ids=[],
        kpis=KpiSummary(
            total_income=Decimal("5000.00"),
            total_expenses=Decimal("3200.00"),
            net_cashflow=Decimal("1800.00"),
            average_daily_spend=Decimal("106.67"),
            savings_rate=Decimal("0.3600"),
            spending_by_category=[],
            top_spending_categories=[],
            month_over_month_spend=None,
            account_balance_snapshots=[],
        ),
        recurring_candidates=[],
        anomaly_events=[],
        meta={"period_rows": 10},
    )


def test_tool_registry_rejects_unsupported_tool() -> None:
    registry = AgentToolRegistry(
        memory_service=cast(Any, _FakeMemoryService()),
        analytics_runner=_fake_analytics_runner,
    )

    with pytest.raises(UnsupportedToolError, match="Unsupported tool"):
        registry.execute_tool(
            conn=cast(Connection, object()),
            user_id=uuid4(),
            tool_name="not_a_real_tool",
            arguments={},
        )


def test_tool_registry_validates_tool_arguments() -> None:
    registry = AgentToolRegistry(
        memory_service=cast(Any, _FakeMemoryService()),
        analytics_runner=_fake_analytics_runner,
    )

    with pytest.raises(ToolValidationError, match="top_k"):
        registry.execute_tool(
            conn=cast(Connection, object()),
            user_id=uuid4(),
            tool_name="search_semantic_memory",
            arguments={"query_text": "spending", "top_k": "three"},
        )

    with pytest.raises(ToolValidationError, match="top_k"):
        registry.execute_tool(
            conn=cast(Connection, object()),
            user_id=uuid4(),
            tool_name="search_semantic_memory",
            arguments={"query_text": "spending", "top_k": True},
        )


def test_tool_registry_returns_structured_kpi_summary() -> None:
    registry = AgentToolRegistry(
        memory_service=cast(Any, _FakeMemoryService()),
        analytics_runner=_fake_analytics_runner,
        goal_lister=lambda conn, user_id, limit: [],
    )

    result = registry.execute_tool(
        conn=cast(Connection, object()),
        user_id=uuid4(),
        tool_name="get_kpi_summary",
        arguments={},
    )

    assert result.tool_name == "get_kpi_summary"
    assert result.output["kpis"]["net_cashflow"] == "1800.00"


def test_tool_registry_recent_goals_is_normalized() -> None:
    goal = SimpleNamespace(
        goal_id=uuid4(),
        title="Emergency Fund",
        description="Build runway",
        status="active",
        target_amount=Decimal("10000.00"),
        target_date=None,
        created_at=datetime(2026, 4, 6, tzinfo=UTC),
    )

    registry = AgentToolRegistry(
        memory_service=cast(Any, _FakeMemoryService()),
        analytics_runner=_fake_analytics_runner,
        goal_lister=lambda conn, user_id, limit: [goal],
    )

    result = registry.execute_tool(
        conn=cast(Connection, object()),
        user_id=uuid4(),
        tool_name="list_recent_goals",
        arguments={"limit": 5},
    )

    assert result.output["count"] == 1
    assert result.output["goals"][0]["target_amount"] == "10000.00"
