from __future__ import annotations

from datetime import datetime

import streamlit as st
from sentinelbudget.db.repositories.insights import Insight

from ui.formatters import format_datetime, severity_label


def render_metric_cards(metrics: list[tuple[str, str, str | None]]) -> None:
    if not metrics:
        return

    st.markdown(
        """
        <style>
        div[data-testid="stMetricValue"] {
            font-size: 1.35rem !important;
            line-height: 1.2 !important;
            white-space: normal !important;
            overflow: visible !important;
            text-overflow: clip !important;
        }
        div[data-testid="stMetricLabel"] {
            white-space: normal !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    columns = st.columns(len(metrics), gap="medium")
    for column, metric in zip(columns, metrics, strict=True):
        label, value, delta = metric
        column.metric(label=label, value=value, delta=delta)


def render_severity_badge(severity: str) -> None:
    normalized = severity.strip().lower()
    colors = {
        "high": "#B91C1C",
        "medium": "#B45309",
        "low": "#166534",
    }
    color = colors.get(normalized, "#334155")
    text = severity_label(severity)
    st.markdown(
        (
            "<span style='display:inline-block;padding:0.2rem 0.5rem;"
            "border-radius:999px;font-size:0.78rem;font-weight:600;"
            f"color:white;background:{color};'>{text}</span>"
        ),
        unsafe_allow_html=True,
    )


def render_insight_card(insight: Insight, show_read_state: bool = True) -> None:
    with st.container(border=True):
        top_left, top_right = st.columns([4, 1])
        with top_left:
            st.markdown(f"### {insight.title}")
            if show_read_state:
                state_label = "Unread" if not insight.is_read else "Read"
                st.caption(f"{state_label} | {format_datetime(insight.created_at)}")
            else:
                st.caption(format_datetime(insight.created_at))
        with top_right:
            render_severity_badge(insight.severity)

        st.write(insight.body)
        with st.expander("Evidence and details"):
            st.json(insight.details)


def render_empty_state(message: str) -> None:
    st.info(message)


def render_section_header(title: str, subtitle: str | None = None) -> None:
    st.subheader(title)
    if subtitle:
        st.caption(subtitle)


def render_last_updated(ts: datetime) -> None:
    st.caption(f"Last refreshed: {format_datetime(ts)}")
