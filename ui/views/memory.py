from __future__ import annotations

from typing import cast
from uuid import UUID

import pandas as pd
import streamlit as st
from sentinelbudget.db.repositories.goals import GoalRepository
from sentinelbudget.memory.models import MemoryKind
from sentinelbudget.memory.repository import SemanticMemoryRepository

from ui.components import render_empty_state, render_section_header
from ui.formatters import format_date, format_datetime, format_money
from ui.state import UIServices, db_transaction


def render(user_id: UUID, services: UIServices) -> None:
    render_section_header(
        "Goals & Memory",
        "Goals, recent semantic memory items, and vector-backed retrieval with metadata.",
    )

    limit = st.slider("Recent memory items", min_value=5, max_value=100, value=20, step=5)

    sync_now = st.button("Sync goals into semantic memory")
    if sync_now:
        try:
            with db_transaction() as conn:
                summary = services.memory_service.sync_goals(conn, user_id=user_id)
            st.success(
                "Goal sync complete: "
                f"processed={summary.processed}, inserted={summary.inserted}, "
                f"updated={summary.updated}, skipped={summary.skipped}"
            )
        except Exception as exc:
            st.warning(f"Goal sync failed: {exc}")

    try:
        with db_transaction() as conn:
            goals = GoalRepository.list_by_user(conn, user_id=user_id, limit=100)
            recent_memory = services.memory_service.list_memory(conn, user_id=user_id, limit=limit)
            embedding_dim = SemanticMemoryRepository.get_embedding_dimension(conn)
    except Exception as exc:
        st.error(f"Unable to load goals/memory data: {exc}")
        return

    st.caption(f"Semantic memory vector dimension: {embedding_dim}")

    st.markdown("### Goals")
    if goals:
        goal_rows = [
            {
                "goal_id": str(goal.goal_id),
                "title": goal.title,
                "status": goal.status,
                "target_amount": (
                    format_money(goal.target_amount) if goal.target_amount is not None else "n/a"
                ),
                "target_date": format_date(goal.target_date),
                "created_at": format_datetime(goal.created_at),
            }
            for goal in goals
        ]
        st.dataframe(pd.DataFrame(goal_rows), width="stretch")
    else:
        render_empty_state("No goals found for this user.")

    st.markdown("### Recent Semantic Memory")
    if recent_memory:
        memory_rows = [
            {
                "id": item.id,
                "kind": item.kind,
                "text": item.text,
                "created_at": format_datetime(item.created_at),
            }
            for item in recent_memory
        ]
        st.dataframe(pd.DataFrame(memory_rows), width="stretch")

        for item in recent_memory:
            with st.expander(f"Memory {item.id} metadata"):
                st.json(item.metadata)
    else:
        render_empty_state("No semantic memory items exist for this user yet.")

    st.markdown("### Semantic Retrieval")
    with st.form("memory-query-form"):
        query_text = st.text_input("Memory query")
        left, right = st.columns(2)
        with left:
            kind_value = st.selectbox(
                "Kind filter",
                options=["all", "goal", "preference", "note"],
            )
        with right:
            top_k = st.number_input("Top K", min_value=1, max_value=20, value=5)

        submitted = st.form_submit_button("Run semantic query")

    if submitted:
        if query_text.strip() == "":
            st.warning("Enter a query before searching semantic memory.")
        else:
            kind_filter: MemoryKind | None = None
            if kind_value != "all":
                kind_filter = cast(MemoryKind, kind_value)

            try:
                with db_transaction() as conn:
                    matches = services.memory_service.query_memory(
                        conn,
                        user_id=user_id,
                        query_text=query_text,
                        kind=kind_filter,
                        top_k=int(top_k),
                    )
            except Exception as exc:
                st.warning(f"Semantic query failed: {exc}")
                return

            if not matches:
                render_empty_state("No semantic matches found for that query.")
            else:
                match_rows = [
                    {
                        "id": item.id,
                        "kind": item.kind,
                        "score": f"{item.score:.4f}",
                        "text": item.text,
                        "created_at": format_datetime(item.created_at),
                    }
                    for item in matches
                ]
                st.dataframe(pd.DataFrame(match_rows), width="stretch")

                for item in matches:
                    with st.expander(f"Match {item.id} metadata"):
                        st.json(item.metadata)
