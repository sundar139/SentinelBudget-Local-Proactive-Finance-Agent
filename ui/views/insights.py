from __future__ import annotations

from uuid import UUID

import streamlit as st

from ui.components import render_empty_state, render_insight_card, render_section_header
from ui.state import UIServices, db_transaction


def render(user_id: UUID, services: UIServices) -> None:
    render_section_header(
        "Insights",
        "Unread insights are shown first with deterministic ordering and evidence details.",
    )

    refresh_now = st.button("Refresh insights", key="insights-refresh")
    if refresh_now:
        st.rerun()

    try:
        with db_transaction() as conn:
            unread = services.review_service.list_unread_insights(conn, user_id=user_id, limit=100)
            recent = services.review_service.list_insights(conn, user_id=user_id, limit=100)
    except Exception as exc:
        st.error(f"Unable to load insights: {exc}")
        return

    st.caption(f"Unread: {len(unread)} | Recent loaded: {len(recent)}")

    if unread:
        unread_ids = [item.id for item in unread]
        selected_to_mark = st.multiselect(
            "Select unread insight IDs to mark read",
            options=unread_ids,
            key="insights-selected-mark-read",
        )

        action_left, action_right = st.columns(2)
        with action_left:
            if st.button("Mark selected as read", disabled=not selected_to_mark):
                try:
                    with db_transaction() as conn:
                        updated = services.review_service.mark_insights_read(
                            conn,
                            user_id=user_id,
                            insight_ids=selected_to_mark,
                        )
                except Exception as exc:
                    st.warning(f"Unable to mark selected insights as read: {exc}")
                else:
                    st.success(f"Marked {updated} insight(s) as read.")
                    st.rerun()

        with action_right:
            if st.button("Mark all unread on page as read"):
                try:
                    with db_transaction() as conn:
                        updated = services.review_service.mark_insights_read(
                            conn,
                            user_id=user_id,
                            insight_ids=unread_ids,
                        )
                except Exception as exc:
                    st.warning(f"Unable to mark unread insights as read: {exc}")
                else:
                    st.success(f"Marked {updated} insight(s) as read.")
                    st.rerun()

        st.markdown("### Unread Insights")
        for insight in unread:
            render_insight_card(insight, show_read_state=True)
            if st.button("Mark read", key=f"insight-mark-one-{insight.id}"):
                try:
                    with db_transaction() as conn:
                        marked = services.review_service.mark_insight_read(
                            conn,
                            user_id=user_id,
                            insight_id=insight.id,
                        )
                except Exception as exc:
                    st.warning(f"Unable to mark insight {insight.id} as read: {exc}")
                else:
                    if marked:
                        st.success(f"Insight {insight.id} marked as read.")
                    else:
                        st.info(f"Insight {insight.id} was already read.")
                    st.rerun()
    else:
        render_empty_state("No unread insights right now.")

    st.markdown("### Recent Insights")
    if recent:
        for insight in recent:
            render_insight_card(insight, show_read_state=True)
    else:
        render_empty_state("No insights have been generated yet.")
