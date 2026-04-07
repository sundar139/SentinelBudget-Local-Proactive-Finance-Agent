from __future__ import annotations

from datetime import UTC, date, datetime
from uuid import UUID

import pandas as pd
import streamlit as st
from sentinelbudget.analytics.cashflow import resolve_time_window
from sentinelbudget.analytics.service import run_analytics
from sentinelbudget.db.repositories.accounts import AccountRepository
from sentinelbudget.db.repositories.ledger import LedgerRepository

from ui.components import (
    render_empty_state,
    render_insight_card,
    render_metric_cards,
    render_section_header,
)
from ui.formatters import format_money, format_percent, format_window_label
from ui.helpers import (
    build_monthly_cashflow_points,
    category_chart_rows,
    count_anomalies_by_severity,
    has_valid_custom_date_range,
)
from ui.state import UIServices, db_transaction

_WINDOW_OPTIONS = ["last_7_days", "last_30_days", "month_to_date", "custom"]


def _window_controls(prefix: str) -> tuple[str, date | None, date | None]:
    window = st.selectbox(
        "Time window",
        options=_WINDOW_OPTIONS,
        index=1,
        format_func=format_window_label,
        key=f"{prefix}-window",
    )

    custom_start: date | None = None
    custom_end: date | None = None

    if window == "custom":
        left, right = st.columns(2)
        with left:
            custom_start = st.date_input("Custom start", key=f"{prefix}-custom-start")
        with right:
            custom_end = st.date_input("Custom end", key=f"{prefix}-custom-end")

        if custom_start > custom_end:
            st.error("Custom start must be on or before custom end.")

    return window, custom_start, custom_end


def render(user_id: UUID, services: UIServices) -> None:
    render_section_header(
        "Overview",
        "Deterministic KPI, category, cashflow, recurring, anomaly, and unread-insight summary.",
    )

    now = datetime.now(UTC)

    with db_transaction() as conn:
        accounts = AccountRepository.list_by_user(conn, user_id)

    account_options = {f"{item.name} ({item.institution})": item.account_id for item in accounts}

    window, custom_start, custom_end = _window_controls("overview")
    if window == "custom" and not has_valid_custom_date_range(custom_start, custom_end):
        render_empty_state("Select a valid custom date range to load overview analytics.")
        return

    selected_account_labels = st.multiselect(
        "Accounts",
        options=list(account_options.keys()),
        key="overview-accounts",
    )
    selected_account_ids = [account_options[label] for label in selected_account_labels]

    try:
        with db_transaction() as conn:
            analytics = run_analytics(
                conn=conn,
                user_id=user_id,
                window=window,
                account_filter_ids=selected_account_ids,
                custom_start=custom_start,
                custom_end=custom_end,
                reference_time=now,
            )
            unread = services.review_service.list_unread_insights(conn, user_id=user_id, limit=3)

            window_bounds = resolve_time_window(
                window,
                reference_time=now,
                custom_start=custom_start,
                custom_end=custom_end,
            )
            ledger_rows = LedgerRepository.query_for_user(
                conn,
                user_id=user_id,
                start_ts=window_bounds.start_ts,
                end_ts=window_bounds.end_ts,
                account_ids=selected_account_ids,
            )
    except Exception as exc:
        st.error(f"Unable to load overview data: {exc}")
        return

    payload = analytics.to_dict()
    kpis = payload["kpis"]

    st.caption(
        "Selected window: "
        f"{format_window_label(window)} | "
        f"{payload['time_window']['start_ts']} to {payload['time_window']['end_ts']}"
    )

    render_metric_cards(
        [
            ("Total Income", format_money(kpis["total_income"]), None),
            ("Total Expenses", format_money(kpis["total_expenses"]), None),
            ("Net Cashflow", format_money(kpis["net_cashflow"]), None),
            ("Savings Rate", format_percent(kpis["savings_rate"]), None),
        ]
    )

    kpi_left, kpi_right = st.columns(2)
    with kpi_left:
        st.metric("Average Daily Spend", format_money(kpis["average_daily_spend"]))
    with kpi_right:
        period_rows = int(payload["meta"].get("period_rows", 0))
        st.metric("Transactions In Window", period_rows)

    st.markdown("### Top Spending Categories")
    top_rows = category_chart_rows(kpis["top_spending_categories"])
    if top_rows:
        top_df = pd.DataFrame(top_rows).set_index("category")
        st.bar_chart(top_df["spend"])
        st.dataframe(top_df, width="stretch")
    else:
        render_empty_state("No spending category data for the selected window.")

    st.markdown("### Cashflow Trend")
    cashflow_points = build_monthly_cashflow_points(ledger_rows)
    if cashflow_points:
        cashflow_df = pd.DataFrame(
            [
                {
                    "month": point.month,
                    "income": float(point.income),
                    "expenses": float(point.expenses),
                    "net": float(point.net),
                }
                for point in cashflow_points
            ]
        ).set_index("month")
        st.line_chart(cashflow_df[["income", "expenses", "net"]])
        st.dataframe(cashflow_df, width="stretch")
    else:
        render_empty_state("No ledger rows found for the selected time window.")

    st.markdown("### Month-over-Month Spend")
    month_over_month = kpis.get("month_over_month_spend")
    if isinstance(month_over_month, dict):
        delta = month_over_month.get("delta_percent")
        st.write(
            "Current period spend: "
            f"{format_money(month_over_month['current_period_spend'])} | "
            "Previous period spend: "
            f"{format_money(month_over_month['previous_period_spend'])} | "
            f"Delta: {format_money(month_over_month['delta_amount'])} "
            f"({format_percent(delta)})"
        )
    else:
        render_empty_state("Not enough prior-window data to compute month-over-month comparison.")

    st.markdown("### Recurring Charge Highlights")
    recurring = payload["recurring_candidates"]
    if recurring:
        recurring_df = pd.DataFrame(recurring)
        st.dataframe(recurring_df.head(10), width="stretch")
    else:
        render_empty_state("No recurring candidates detected in current baseline data.")

    st.markdown("### Anomaly Summary")
    anomalies = payload["anomaly_events"]
    if anomalies:
        counts = count_anomalies_by_severity(anomalies)
        severity_columns = st.columns(4)
        severity_columns[0].metric("High", counts["high"])
        severity_columns[1].metric("Medium", counts["medium"])
        severity_columns[2].metric("Low", counts["low"])
        severity_columns[3].metric("Unknown", counts["unknown"])

        anomaly_df = pd.DataFrame(anomalies)
        st.dataframe(anomaly_df.head(20), width="stretch")
    else:
        render_empty_state("No anomalies detected for the selected period.")

    st.markdown("### Recent Unread Insights")
    if unread:
        for insight in unread:
            render_insight_card(insight, show_read_state=True)
    else:
        render_empty_state("No unread proactive insights.")
