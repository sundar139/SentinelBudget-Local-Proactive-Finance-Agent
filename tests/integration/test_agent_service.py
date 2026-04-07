from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from uuid import uuid4

import pytest
from psycopg import Connection
from sentinelbudget.agent.history import PostgresConversationHistoryStore
from sentinelbudget.agent.models import ChatModelResult, ChatToolCall
from sentinelbudget.agent.orchestrator import ConversationOrchestrator
from sentinelbudget.agent.provider import FakeDeterministicChatModelProvider
from sentinelbudget.agent.service import SentinelBudgetChatService
from sentinelbudget.agent.tools import AgentToolRegistry
from sentinelbudget.config import get_settings
from sentinelbudget.db.repositories import AccountRepository, UserRepository
from sentinelbudget.db.schema import bootstrap_default_categories
from sentinelbudget.ingest.service import ingest_synthetic_transactions
from sentinelbudget.memory.embeddings import DummyDeterministicEmbeddingProvider
from sentinelbudget.memory.repository import SemanticMemoryRepository
from sentinelbudget.memory.service import SemanticMemoryService


@pytest.mark.integration
def test_conversation_history_persists_in_order(db_conn: Connection) -> None:
    user = UserRepository.create(db_conn, email=f"chat-history-{uuid4()}@example.com")
    session_id = uuid4()

    store = PostgresConversationHistoryStore()
    store.append_message(
        db_conn,
        session_id=session_id,
        user_id=user.user_id,
        role="user",
        content="hello",
        metadata={"turn": 1},
    )
    store.append_message(
        db_conn,
        session_id=session_id,
        user_id=user.user_id,
        role="assistant",
        content="hi",
        metadata={"turn": 1},
    )
    store.append_message(
        db_conn,
        session_id=session_id,
        user_id=user.user_id,
        role="tool",
        content='{"ok": true}',
        metadata={"tool_name": "get_kpi_summary"},
    )

    rows = store.list_recent(db_conn, session_id=session_id, user_id=user.user_id, limit=10)

    assert [row.role for row in rows] == ["user", "assistant", "tool"]
    assert [row.content for row in rows] == ["hello", "hi", '{"ok": true}']


@pytest.mark.integration
def test_conversation_history_rejects_non_object_metadata(db_conn: Connection) -> None:
    user = UserRepository.create(db_conn, email=f"chat-history-meta-{uuid4()}@example.com")
    session_id = uuid4()

    store = PostgresConversationHistoryStore()
    with pytest.raises(ValueError, match="metadata must be a JSON object"):
        store.append_message(
            db_conn,
            session_id=session_id,
            user_id=user.user_id,
            role="user",
            content="hello",
            metadata=[],  # type: ignore[arg-type]
        )


@pytest.mark.integration
def test_orchestration_with_analytics_and_memory_tools(db_conn: Connection) -> None:
    bootstrap_default_categories(db_conn)

    user = UserRepository.create(db_conn, email=f"chat-agent-{uuid4()}@example.com")
    account = AccountRepository.create(
        db_conn,
        user_id=user.user_id,
        institution="Test Bank",
        name="Primary Checking",
        account_type="checking",
        current_balance=Decimal("1000.00"),
    )

    ingest_synthetic_transactions(
        conn=db_conn,
        account_id=account.account_id,
        days=90,
        seed=21,
        start_date=date(2026, 1, 1),
        source_dataset="agent-int",
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
        text="User wants to cut dining expenses next month",
        metadata={"source": "manual_note"},
    )

    provider = FakeDeterministicChatModelProvider(
        scripted_responses=[
            ChatModelResult(
                content=None,
                tool_call=ChatToolCall(
                    name="get_kpi_summary",
                    arguments={"window": "last_30_days"},
                ),
            ),
            ChatModelResult(
                content=None,
                tool_call=ChatToolCall(
                    name="search_semantic_memory",
                    arguments={"query_text": "dining expenses", "top_k": 3},
                ),
            ),
            ChatModelResult(
                content=(
                    '{"answer_text":"You appear cashflow positive in the last 30 days and '\
                    'you have a stored note about cutting dining expenses.",' \
                    '"citations":[{"tool_name":"get_kpi_summary","evidence":"Used KPI"},' \
                    '{"tool_name":"search_semantic_memory","evidence":"Used memory"}],'
                    '"warnings":[],"structured_payload":{"window":"last_30_days"}}'
                ),
                tool_call=None,
            ),
        ]
    )

    registry = AgentToolRegistry(memory_service=memory_service)
    orchestrator = ConversationOrchestrator(
        provider=provider,
        history_store=PostgresConversationHistoryStore(),
        tool_registry=registry,
    )

    session_id = uuid4()
    answer = orchestrator.run_turn(
        db_conn,
        user_id=user.user_id,
        session_id=session_id,
        user_message="Am I overspending on dining?",
    )

    assert answer.tools_used == ["get_kpi_summary", "search_semantic_memory"]
    assert len(answer.citations) == 2
    assert answer.structured_payload is not None

    history_rows = PostgresConversationHistoryStore().list_recent(
        db_conn,
        session_id=session_id,
        user_id=user.user_id,
        limit=20,
    )

    assert history_rows[0].role == "user"
    assert history_rows[-1].role == "assistant"


@pytest.mark.integration
def test_chat_service_ask_runtime_path_persists_history(
    db_conn: Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user = UserRepository.create(db_conn, email=f"chat-service-{uuid4()}@example.com")
    session_id = uuid4()

    dimension = SemanticMemoryRepository.get_embedding_dimension(db_conn)
    memory_service = SemanticMemoryService(
        DummyDeterministicEmbeddingProvider(dimension=dimension),
        embedding_dimension=dimension,
    )

    provider = FakeDeterministicChatModelProvider(
        scripted_responses=[
            ChatModelResult(
                content='{"answer_text":"All good.","citations":[],"warnings":[]}',
                tool_call=None,
            )
        ]
    )

    service = SentinelBudgetChatService(
        settings=get_settings(),
        provider=provider,
        memory_service=memory_service,
    )

    @contextmanager
    def _same_connection_transaction(_settings):
        yield db_conn

    monkeypatch.setattr("sentinelbudget.agent.service.transaction", _same_connection_transaction)

    answer = service.ask(
        user_id=user.user_id,
        session_id=session_id,
        message="Quick health check",
    )

    assert answer.answer_text == "All good."

    history_rows = service.inspect_session(
        user_id=user.user_id,
        session_id=session_id,
        limit=10,
    )
    assert [item.role for item in history_rows] == ["user", "assistant"]


@pytest.mark.integration
def test_chat_service_grounds_answer_with_placeholder_account_ids(
    db_conn: Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bootstrap_default_categories(db_conn)

    user = UserRepository.create(db_conn, email=f"chat-grounded-{uuid4()}@example.com")
    account = AccountRepository.create(
        db_conn,
        user_id=user.user_id,
        institution="Grounded Bank",
        name="Main",
        account_type="checking",
        current_balance=Decimal("500.00"),
    )
    ingest_synthetic_transactions(
        conn=db_conn,
        account_id=account.account_id,
        days=45,
        seed=12,
        start_date=date(2026, 2, 1),
        source_dataset="chat-grounded",
        output_csv=None,
    )

    session_id = uuid4()
    dimension = SemanticMemoryRepository.get_embedding_dimension(db_conn)
    memory_service = SemanticMemoryService(
        DummyDeterministicEmbeddingProvider(dimension=dimension),
        embedding_dimension=dimension,
    )
    provider = FakeDeterministicChatModelProvider(
        scripted_responses=[
            ChatModelResult(
                content=None,
                tool_call=ChatToolCall(
                    name="get_category_summary",
                    arguments={
                        "window": "last_30_days",
                        "account_ids": ["user_account_id"],
                    },
                ),
            ),
            ChatModelResult(
                content='{"citations": [], "warnings": [], "structured_payload": {}}',
                tool_call=None,
            ),
        ]
    )

    service = SentinelBudgetChatService(
        settings=get_settings(),
        provider=provider,
        memory_service=memory_service,
    )

    @contextmanager
    def _same_connection_transaction(_settings):
        yield db_conn

    monkeypatch.setattr("sentinelbudget.agent.service.transaction", _same_connection_transaction)

    answer = service.ask(
        user_id=user.user_id,
        session_id=session_id,
        message="Show me category spend this month",
    )

    assert answer.tools_used == ["get_category_spend"]
    assert len(answer.citations) == 1
    assert answer.citations[0].payload is not None
    assert "Based on grounded category-spend results" in answer.answer_text
