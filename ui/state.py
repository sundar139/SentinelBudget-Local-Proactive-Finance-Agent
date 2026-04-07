from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from uuid import UUID, uuid4

import streamlit as st
from psycopg import Connection
from sentinelbudget.agent.service import SentinelBudgetChatService, build_chat_service
from sentinelbudget.config import Settings, get_settings
from sentinelbudget.db.repositories.session import transaction
from sentinelbudget.memory.embeddings import OllamaEmbeddingProvider
from sentinelbudget.memory.service import SemanticMemoryService
from sentinelbudget.review.service import ProactiveReviewService


@dataclass(frozen=True, slots=True)
class UIServices:
    settings: Settings
    memory_service: SemanticMemoryService
    review_service: ProactiveReviewService
    chat_service: SentinelBudgetChatService


def parse_uuid_text(raw: str) -> UUID | None:
    cleaned = raw.strip()
    if cleaned == "":
        return None
    try:
        return UUID(cleaned)
    except ValueError:
        return None


def ensure_session_state_defaults() -> None:
    st.session_state.setdefault("ui_active_user_id", "")
    st.session_state.setdefault("ui_recent_user_ids", [])
    st.session_state.setdefault("ui_chat_session_id", str(uuid4()))
    st.session_state.setdefault("ui_unread_count", None)
    st.session_state.setdefault("ui_previous_unread_count", None)
    st.session_state.setdefault("ui_sidebar_flash", None)


def set_active_user_id(user_id: UUID) -> None:
    user_text = str(user_id)
    st.session_state["ui_active_user_id"] = user_text

    recent: list[str] = list(st.session_state.get("ui_recent_user_ids", []))
    if user_text in recent:
        recent.remove(user_text)
    recent.insert(0, user_text)
    st.session_state["ui_recent_user_ids"] = recent[:10]


def get_active_user_id() -> UUID | None:
    raw = str(st.session_state.get("ui_active_user_id", ""))
    return parse_uuid_text(raw)


def get_recent_user_ids() -> list[str]:
    recent = st.session_state.get("ui_recent_user_ids", [])
    if not isinstance(recent, list):
        return []
    return [str(item) for item in recent]


def set_chat_session_id(session_id: UUID) -> None:
    st.session_state["ui_chat_session_id"] = str(session_id)


def get_chat_session_id() -> UUID | None:
    raw = str(st.session_state.get("ui_chat_session_id", ""))
    return parse_uuid_text(raw)


def reset_chat_session_id() -> UUID:
    new_id = uuid4()
    set_chat_session_id(new_id)
    return new_id


@st.cache_resource
def get_ui_services() -> UIServices:
    settings = get_settings()
    memory_provider = OllamaEmbeddingProvider(
        base_url=str(settings.ollama_base_url),
        model=settings.memory_embedding_model,
        dimension=settings.memory_embedding_dim,
        timeout_seconds=settings.memory_embedding_timeout_seconds,
    )
    memory_service = SemanticMemoryService(
        embedding_provider=memory_provider,
        embedding_dimension=settings.memory_embedding_dim,
    )
    review_service = ProactiveReviewService(settings=settings, memory_service=memory_service)
    chat_service = build_chat_service(settings=settings, memory_service=memory_service)

    return UIServices(
        settings=settings,
        memory_service=memory_service,
        review_service=review_service,
        chat_service=chat_service,
    )


@contextmanager
def db_transaction() -> Iterator[Connection]:
    services = get_ui_services()
    with transaction(services.settings) as conn:
        yield conn


def fetch_unread_count(user_id: UUID) -> int:
    with db_transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM insights
                WHERE user_id = %s
                  AND is_read = false;
                """,
                (user_id,),
            )
            row = cur.fetchone()

    if row is None:
        return 0
    return int(row[0])


def update_unread_count(user_id: UUID) -> int:
    count = fetch_unread_count(user_id)
    previous = st.session_state.get("ui_unread_count")
    st.session_state["ui_previous_unread_count"] = previous
    st.session_state["ui_unread_count"] = count
    return count


def get_unread_count_state() -> int | None:
    value = st.session_state.get("ui_unread_count")
    if isinstance(value, int):
        return value
    return None


def get_previous_unread_count_state() -> int | None:
    value = st.session_state.get("ui_previous_unread_count")
    if isinstance(value, int):
        return value
    return None


def set_sidebar_flash(message: str, level: str = "success") -> None:
    normalized_message = message.strip()
    if normalized_message == "":
        st.session_state["ui_sidebar_flash"] = None
        return

    st.session_state["ui_sidebar_flash"] = {
        "level": level,
        "message": normalized_message,
    }


def pop_sidebar_flash() -> tuple[str, str] | None:
    raw_value = st.session_state.get("ui_sidebar_flash")
    st.session_state["ui_sidebar_flash"] = None

    if not isinstance(raw_value, dict):
        return None

    level = raw_value.get("level")
    message = raw_value.get("message")
    if not isinstance(level, str) or not isinstance(message, str):
        return None
    if message.strip() == "":
        return None

    return level, message
