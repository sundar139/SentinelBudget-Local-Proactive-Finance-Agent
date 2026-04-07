from __future__ import annotations

from collections.abc import Callable
from typing import cast
from uuid import UUID

import streamlit as st
from sentinelbudget.db.repositories.users import UserRepository
from sentinelbudget.logging import setup_logging

from ui.state import (
    db_transaction,
    ensure_session_state_defaults,
    get_active_user_id,
    get_previous_unread_count_state,
    get_recent_user_ids,
    get_ui_services,
    get_unread_count_state,
    parse_uuid_text,
    set_active_user_id,
    update_unread_count,
)
from ui.views import chat, insights, memory, overview, settings, transactions

_PAGE_REGISTRY = {
    "Overview": overview.render,
    "Transactions": transactions.render,
    "Insights": insights.render,
    "Goals & Memory": memory.render,
    "Chat": chat.render,
    "Settings / Debug": settings.render,
}


def _render_sidebar_user_controls() -> UUID | None:
    st.sidebar.header("SentinelBudget")
    st.sidebar.caption("Local-first deterministic finance dashboard")

    input_value = st.sidebar.text_input(
        "Active user UUID",
        value=str(st.session_state.get("ui_active_user_id", "")),
        key="sidebar-active-user-id-input",
    )

    apply_clicked = st.sidebar.button("Set active user")
    if apply_clicked:
        parsed = parse_uuid_text(input_value)
        if parsed is None:
            st.sidebar.error("Enter a valid UUID.")
        else:
            set_active_user_id(parsed)
            st.sidebar.success("Active user updated.")

    recent_user_ids = get_recent_user_ids()
    if recent_user_ids:
        selected_recent = st.sidebar.selectbox(
            "Recent users",
            options=["", *recent_user_ids],
            key="sidebar-recent-users-select",
        )
        if st.sidebar.button("Use selected recent", disabled=selected_recent == ""):
            parsed_recent = parse_uuid_text(selected_recent)
            if parsed_recent is not None:
                set_active_user_id(parsed_recent)
                st.rerun()

    return get_active_user_id()


def _render_unread_shell(active_user_id: UUID) -> None:
    unread_error: str | None = None

    if st.sidebar.button("Refresh unread insights now"):
        try:
            update_unread_count(active_user_id)
        except Exception as exc:
            unread_error = str(exc)

    if get_unread_count_state() is None or st.session_state.get("ui_unread_user_id") != str(
        active_user_id
    ):
        try:
            update_unread_count(active_user_id)
        except Exception as exc:
            unread_error = str(exc)
            st.session_state["ui_unread_count"] = 0
        st.session_state["ui_unread_user_id"] = str(active_user_id)

    unread_count = get_unread_count_state() or 0
    previous_count = get_previous_unread_count_state()

    st.sidebar.metric("Unread insights", unread_count)
    if unread_error is not None:
        st.sidebar.warning(f"Unread insight count unavailable: {unread_error}")

    if previous_count is not None and unread_count > previous_count:
        st.sidebar.caption(f"{unread_count - previous_count} new unread insight(s).")

    auto_refresh = st.sidebar.checkbox(
        "Live unread polling",
        value=False,
        help="Uses Streamlit fragment polling when available, otherwise manual refresh.",
    )
    poll_seconds = st.sidebar.slider("Polling interval (seconds)", 10, 120, 30, 5)

    fragment_factory = getattr(st, "fragment", None)
    if auto_refresh and callable(fragment_factory):
        typed_fragment_factory = cast(
            Callable[..., Callable[[Callable[[], None]], Callable[[], None]]],
            fragment_factory,
        )

        @typed_fragment_factory(run_every=f"{poll_seconds}s")
        def poll_unread_fragment() -> None:
            try:
                latest = update_unread_count(active_user_id)
                st.caption(f"Live unread count: {latest}")
            except Exception as exc:
                st.caption(f"Live unread count unavailable: {exc}")

        with st.sidebar.expander("Live insight surfacing", expanded=False):
            poll_unread_fragment()
    elif auto_refresh:
        st.sidebar.info("Streamlit version lacks st.fragment; use manual refresh button.")


def main() -> None:
    st.set_page_config(page_title="SentinelBudget", page_icon="SB", layout="wide")
    ensure_session_state_defaults()
    logger = setup_logging()

    logger.info("Streamlit UI bootstrap started", extra={"command": "ui-app"})

    try:
        services = get_ui_services()
    except Exception as exc:
        logger.error(
            "UI services initialization failed",
            extra={"command": "ui-app", "detail": str(exc)},
        )
        st.error(f"Unable to initialize UI services: {exc}")
        st.stop()

    active_user_id = _render_sidebar_user_controls()
    if active_user_id is None:
        st.title("SentinelBudget UI")
        st.info("Enter a valid user UUID in the sidebar to load dashboard data.")
        st.stop()

    logger.info(
        "Streamlit UI active user resolved",
        extra={"command": "ui-app", "user_id": str(active_user_id)},
    )

    _render_unread_shell(active_user_id)

    user_email: str | None = None
    try:
        with db_transaction() as conn:
            user = UserRepository.get_by_id(conn, active_user_id)
        if user is not None:
            user_email = user.email
        else:
            st.warning(
                "No user row found for the active UUID. "
                "Pages may show empty-state results until data is loaded."
            )
    except Exception as exc:
        st.warning(f"Unable to verify active user profile: {exc}")

    page = st.sidebar.radio("Page", options=list(_PAGE_REGISTRY.keys()))

    st.title("SentinelBudget")
    if user_email is None:
        st.caption(f"Active user: {active_user_id}")
    else:
        st.caption(f"Active user: {active_user_id} ({user_email})")

    renderer = _PAGE_REGISTRY[page]
    renderer(active_user_id, services)


if __name__ == "__main__":
    main()
