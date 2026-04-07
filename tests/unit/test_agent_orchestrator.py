from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID, uuid4

from psycopg import Connection
from sentinelbudget.agent.history import ConversationHistoryStore
from sentinelbudget.agent.models import (
    ChatModelResult,
    ChatToolCall,
    ConversationEntry,
)
from sentinelbudget.agent.orchestrator import ConversationOrchestrator
from sentinelbudget.agent.provider import (
    ChatProviderUnavailableError,
    FakeDeterministicChatModelProvider,
)
from sentinelbudget.agent.tools import AgentToolRegistry


class _FakeMemoryService:
    def query_memory(
        self,
        conn: Connection,
        user_id: UUID,
        query_text: str,
        kind: str | None,
        top_k: int,
    ) -> list[Any]:
        del conn, user_id, query_text, kind, top_k
        return []


def _fake_analytics_runner(**kwargs: object) -> Any:
    del kwargs

    class _Result:
        def to_dict(self) -> dict[str, Any]:
            return {
                "time_window": {"label": "last_30_days"},
                "kpis": {
                    "total_income": "4000.00",
                    "total_expenses": "2500.00",
                    "net_cashflow": "1500.00",
                    "spending_by_category": [],
                    "top_spending_categories": [],
                },
                "recurring_candidates": [],
                "anomaly_events": [],
                "meta": {"period_rows": 5},
            }

    return _Result()


@dataclass(slots=True)
class _InMemoryHistoryStore(ConversationHistoryStore):
    rows: list[ConversationEntry] = field(default_factory=list)
    _next_id: int = 1

    def append_message(
        self,
        conn: Connection,
        session_id: UUID,
        user_id: UUID,
        role: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> ConversationEntry:
        del conn
        entry = ConversationEntry(
            id=self._next_id,
            session_id=session_id,
            user_id=user_id,
            role=cast(Any, role),
            content=content,
            metadata=metadata or {},
            created_at=datetime(2026, 4, 6, 12, 0, self._next_id, tzinfo=UTC),
        )
        self.rows.append(entry)
        self._next_id += 1
        return entry

    def list_recent(
        self,
        conn: Connection,
        session_id: UUID,
        user_id: UUID,
        limit: int = 40,
    ) -> list[ConversationEntry]:
        del conn
        filtered = [
            row
            for row in self.rows
            if row.session_id == session_id and row.user_id == user_id
        ]
        return filtered[-limit:]


def test_orchestrator_happy_path_with_tool_call() -> None:
    provider = FakeDeterministicChatModelProvider(
        scripted_responses=[
            ChatModelResult(
                content=None,
                tool_call=ChatToolCall(name="get_kpi_summary", arguments={}),
            ),
            ChatModelResult(
                content=(
                    '{"answer_text":"You are cashflow positive.",' \
                    '"citations":[{"tool_name":"get_kpi_summary","evidence":"Used KPI summary"}],' \
                    '"warnings":[],"structured_payload":{"net_cashflow":"1500.00"}}'
                ),
                tool_call=None,
            ),
        ]
    )
    history = _InMemoryHistoryStore()
    registry = AgentToolRegistry(
        memory_service=cast(Any, _FakeMemoryService()),
        analytics_runner=_fake_analytics_runner,
        goal_lister=lambda conn, user_id, limit: [],
    )

    orchestrator = ConversationOrchestrator(
        provider=provider,
        history_store=history,
        tool_registry=registry,
    )

    answer = orchestrator.run_turn(
        conn=cast(Connection, object()),
        user_id=uuid4(),
        session_id=uuid4(),
        user_message="Am I overspending?",
    )

    assert answer.answer_text == "You are cashflow positive."
    assert answer.tools_used == ["get_kpi_summary"]
    assert answer.citations[0].tool_name == "get_kpi_summary"


def test_orchestrator_rejects_unsupported_tool_call_safely() -> None:
    provider = FakeDeterministicChatModelProvider(
        scripted_responses=[
            ChatModelResult(
                content=None,
                tool_call=ChatToolCall(name="delete_all_data", arguments={}),
            ),
            ChatModelResult(
                content='{"answer_text":"I cannot do that.","citations":[],"warnings":[]}',
                tool_call=None,
            ),
        ]
    )
    history = _InMemoryHistoryStore()
    registry = AgentToolRegistry(
        memory_service=cast(Any, _FakeMemoryService()),
        analytics_runner=_fake_analytics_runner,
        goal_lister=lambda conn, user_id, limit: [],
    )
    orchestrator = ConversationOrchestrator(
        provider=provider,
        history_store=history,
        tool_registry=registry,
    )

    answer = orchestrator.run_turn(
        conn=cast(Connection, object()),
        user_id=uuid4(),
        session_id=uuid4(),
        user_message="Drop all tables",
    )

    assert any("Unsupported tool" in warning for warning in answer.warnings)


def test_orchestrator_handles_model_unavailable() -> None:
    provider = FakeDeterministicChatModelProvider(
        scripted_responses=[],
        errors_by_call={1: ChatProviderUnavailableError("offline")},
    )
    history = _InMemoryHistoryStore()
    registry = AgentToolRegistry(
        memory_service=cast(Any, _FakeMemoryService()),
        analytics_runner=_fake_analytics_runner,
        goal_lister=lambda conn, user_id, limit: [],
    )
    orchestrator = ConversationOrchestrator(
        provider=provider,
        history_store=history,
        tool_registry=registry,
    )

    answer = orchestrator.run_turn(
        conn=cast(Connection, object()),
        user_id=uuid4(),
        session_id=uuid4(),
        user_message="What is my balance?",
    )

    assert "could not complete the request safely" in answer.answer_text.lower()
    assert any("Local model unavailable" in warning for warning in answer.warnings)


def test_orchestrator_handles_malformed_tool_arguments() -> None:
    provider = FakeDeterministicChatModelProvider(
        scripted_responses=[
            ChatModelResult(
                content=None,
                tool_call=ChatToolCall(
                    name="get_kpi_summary",
                    arguments={"window": "definitely_not_valid"},
                ),
            ),
            ChatModelResult(
                content='{"answer_text":"Need corrected input.","citations":[],"warnings":[]}',
                tool_call=None,
            ),
        ]
    )
    history = _InMemoryHistoryStore()
    registry = AgentToolRegistry(
        memory_service=cast(Any, _FakeMemoryService()),
        analytics_runner=_fake_analytics_runner,
        goal_lister=lambda conn, user_id, limit: [],
    )
    orchestrator = ConversationOrchestrator(
        provider=provider,
        history_store=history,
        tool_registry=registry,
    )

    answer = orchestrator.run_turn(
        conn=cast(Connection, object()),
        user_id=uuid4(),
        session_id=uuid4(),
        user_message="Show me KPI",
    )

    assert any("Invalid arguments" in warning for warning in answer.warnings)


def test_orchestrator_uses_grounded_fallback_for_unstructured_final_output() -> None:
    provider = FakeDeterministicChatModelProvider(
        scripted_responses=[
            ChatModelResult(
                content=None,
                tool_call=ChatToolCall(name="get_kpi_summary", arguments={}),
            ),
            ChatModelResult(
                content="You are definitely great financially.",
                tool_call=None,
            ),
        ]
    )
    history = _InMemoryHistoryStore()
    registry = AgentToolRegistry(
        memory_service=cast(Any, _FakeMemoryService()),
        analytics_runner=_fake_analytics_runner,
        goal_lister=lambda conn, user_id, limit: [],
    )
    orchestrator = ConversationOrchestrator(
        provider=provider,
        history_store=history,
        tool_registry=registry,
    )

    answer = orchestrator.run_turn(
        conn=cast(Connection, object()),
        user_id=uuid4(),
        session_id=uuid4(),
        user_message="How is my budget?",
    )

    assert "Based on grounded KPI results" in answer.answer_text
    assert any("deterministic grounded fallback" in warning for warning in answer.warnings)


def test_orchestrator_uses_grounded_fallback_when_structured_answer_text_missing() -> None:
    provider = FakeDeterministicChatModelProvider(
        scripted_responses=[
            ChatModelResult(
                content=None,
                tool_call=ChatToolCall(
                    name="get_category_summary",
                    arguments={"account_ids": ["user_account_id"]},
                ),
            ),
            ChatModelResult(
                content='{"citations": [], "warnings": [], "structured_payload": {}}',
                tool_call=None,
            ),
        ]
    )
    history = _InMemoryHistoryStore()
    registry = AgentToolRegistry(
        memory_service=cast(Any, _FakeMemoryService()),
        analytics_runner=_fake_analytics_runner,
        goal_lister=lambda conn, user_id, limit: [],
    )
    orchestrator = ConversationOrchestrator(
        provider=provider,
        history_store=history,
        tool_registry=registry,
    )

    answer = orchestrator.run_turn(
        conn=cast(Connection, object()),
        user_id=uuid4(),
        session_id=uuid4(),
        user_message="How much did I spend by category?",
    )

    assert "Based on grounded category-spend results" in answer.answer_text
    assert answer.tools_used == ["get_category_spend"]
    assert len(answer.citations) == 1
    assert answer.citations[0].payload is not None
