from __future__ import annotations

import json
from uuid import UUID, uuid4

import streamlit as st
from sentinelbudget.agent.models import ConversationEntry

from ui.components import render_section_header
from ui.state import (
    UIServices,
    get_chat_session_id,
    parse_uuid_text,
    set_chat_session_id,
)


def _render_assistant_metadata(entry: ConversationEntry) -> None:
    warnings_obj = entry.metadata.get("warnings")
    tools_used_obj = entry.metadata.get("tools_used")
    citations_obj = entry.metadata.get("citations")

    warnings = warnings_obj if isinstance(warnings_obj, list) else []
    tools_used = tools_used_obj if isinstance(tools_used_obj, list) else []
    citations = citations_obj if isinstance(citations_obj, list) else []

    if warnings or tools_used or citations:
        with st.expander("Provenance and warnings"):
            if warnings:
                st.write("Warnings")
                for warning in warnings:
                    st.warning(str(warning))
            if tools_used:
                st.write("Tools used")
                st.code(json.dumps(tools_used, indent=2, sort_keys=True), language="json")
            if citations:
                st.write("Citations")
                st.code(json.dumps(citations, indent=2, sort_keys=True), language="json")


def _render_history(entries: list[ConversationEntry]) -> None:
    for entry in entries:
        if entry.role == "user":
            with st.chat_message("user"):
                st.write(entry.content)
            continue

        if entry.role == "assistant":
            metadata_kind = entry.metadata.get("kind")
            with st.chat_message("assistant"):
                if metadata_kind == "tool_call":
                    st.caption("Tool call event")
                    st.code(entry.content, language="json")
                else:
                    st.write(entry.content)
                _render_assistant_metadata(entry)
            continue

    tool_events = [item for item in entries if item.role == "tool"]
    if tool_events:
        with st.expander("Tool trace events"):
            for item in tool_events:
                st.caption(str(item.created_at))
                st.code(item.content, language="json")


def render(user_id: UUID, services: UIServices) -> None:
    render_section_header(
        "Chat",
        "Grounded chat backed by persisted conversation history and existing tool orchestration.",
    )

    current_session = get_chat_session_id() or uuid4()
    if get_chat_session_id() is None:
        set_chat_session_id(current_session)

    session_col, control_col = st.columns([3, 1])
    with session_col:
        session_input = st.text_input(
            "Session ID",
            value=str(current_session),
            key="chat-session-id-input",
        )
    with control_col:
        if st.button("New session"):
            new_session = uuid4()
            set_chat_session_id(new_session)
            st.rerun()

    parsed_session = parse_uuid_text(session_input)
    if parsed_session is None:
        st.error("Session ID must be a valid UUID.")
        return

    set_chat_session_id(parsed_session)

    history_limit = st.slider("History rows", min_value=10, max_value=100, value=40, step=5)

    try:
        entries = services.chat_service.inspect_session(
            user_id=user_id,
            session_id=parsed_session,
            limit=history_limit,
        )
    except Exception as exc:
        st.warning(f"Unable to load chat history: {exc}")
        entries = []

    _render_history(entries)

    prompt = st.chat_input("Ask about spending, anomalies, goals, or recurring patterns")
    if prompt:
        with st.spinner("Running grounded chat..."):
            try:
                answer = services.chat_service.ask(
                    user_id=user_id,
                    session_id=parsed_session,
                    message=prompt,
                )
            except Exception as exc:
                st.error(f"Chat request failed: {exc}")
                return

        with st.chat_message("assistant"):
            st.write(answer.answer_text)
            if answer.warnings:
                for warning in answer.warnings:
                    st.warning(warning)
            if answer.citations:
                with st.expander("Evidence and tool citations"):
                    for citation in answer.citations:
                        st.markdown(f"**{citation.tool_name}**")
                        st.write(citation.evidence)
                        if citation.payload is not None:
                            st.json(citation.payload)

        st.rerun()
