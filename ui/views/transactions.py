from __future__ import annotations

from datetime import UTC, date, datetime
from uuid import UUID

import pandas as pd
import streamlit as st
from sentinelbudget.analytics.cashflow import resolve_time_window
from sentinelbudget.analytics.service import run_analytics
from sentinelbudget.db.repositories.accounts import AccountRepository
from sentinelbudget.db.repositories.ledger import LedgerRepository

from ui.components import render_empty_state, render_section_header
from ui.formatters import format_window_label
from ui.helpers import (
    build_transaction_records,
    filter_transaction_records,
    has_valid_custom_date_range,
    records_to_csv,
    transaction_records_to_rows,
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
        "Transactions",
        "Filter deterministic ledger rows by account, category, date, direction, and text.",
    )

    now = datetime.now(UTC)

    with db_transaction() as conn:
        accounts = AccountRepository.list_by_user(conn, user_id)

    account_options = {f"{item.name} ({item.institution})": item.account_id for item in accounts}

    window, custom_start, custom_end = _window_controls("transactions")
    if window == "custom" and not has_valid_custom_date_range(custom_start, custom_end):
        render_empty_state("Select a valid custom date range to load transactions.")
        return

    selected_account_labels = st.multiselect(
        "Accounts",
        options=list(account_options.keys()),
        key="transactions-accounts",
    )
    selected_account_ids = [account_options[label] for label in selected_account_labels]

    try:
        with db_transaction() as conn:
            window_bounds = resolve_time_window(
                window,
                reference_time=now,
                custom_start=custom_start,
                custom_end=custom_end,
            )
            rows = LedgerRepository.query_for_user(
                conn,
                user_id=user_id,
                start_ts=window_bounds.start_ts,
                end_ts=window_bounds.end_ts,
                account_ids=selected_account_ids,
            )

            analytics = run_analytics(
                conn=conn,
                user_id=user_id,
                window=window,
                account_filter_ids=selected_account_ids,
                custom_start=custom_start,
                custom_end=custom_end,
                reference_time=now,
            )
    except Exception as exc:
        st.error(f"Unable to load transaction data: {exc}")
        return

    anomaly_events = analytics.to_dict()["anomaly_events"]
    anomaly_keys = {
        int(item["trans_key"])
        for item in anomaly_events
        if isinstance(item, dict) and item.get("trans_key") is not None
    }

    records = build_transaction_records(rows, anomaly_trans_keys=anomaly_keys)

    if not records:
        render_empty_state("No transactions found for this selection.")
        return

    available_accounts = sorted({item.account_name for item in records})
    available_categories = sorted({item.category_name for item in records})

    filter_left, filter_right = st.columns(2)
    with filter_left:
        account_filter = set(
            st.multiselect(
                "Account filter",
                options=available_accounts,
                default=available_accounts,
                key="transactions-account-filter",
            )
        )
        category_filter = set(
            st.multiselect(
                "Category filter",
                options=available_categories,
                default=available_categories,
                key="transactions-category-filter",
            )
        )

    with filter_right:
        direction_filter = set(
            st.multiselect(
                "Direction",
                options=["inflow", "outflow"],
                default=["inflow", "outflow"],
                key="transactions-direction-filter",
            )
        )
        anomalies_only = st.checkbox(
            "Show anomalies only (from analytics output)",
            value=False,
            key="transactions-anomalies-only",
        )

    search_text = st.text_input("Text search", key="transactions-search")

    filtered = filter_transaction_records(
        records,
        account_names=account_filter,
        categories=category_filter,
        directions=direction_filter,
        search_text=search_text,
        anomalies_only=anomalies_only,
    )

    capped_rows = st.slider(
        "Rows to display",
        min_value=25,
        max_value=500,
        value=200,
        step=25,
    )

    shown_records = filtered[:capped_rows]
    table_rows = transaction_records_to_rows(shown_records)

    if not table_rows:
        render_empty_state("No transactions match the selected filters.")
        return

    st.caption(
        f"Showing {len(shown_records)} of {len(filtered)} filtered transactions "
        f"for {format_window_label(window)}."
    )

    df = pd.DataFrame(table_rows)
    st.dataframe(df, width="stretch")

    csv_payload = records_to_csv(table_rows)
    st.download_button(
        "Export filtered view as CSV",
        data=csv_payload,
        file_name="sentinelbudget_transactions_filtered.csv",
        mime="text/csv",
    )
