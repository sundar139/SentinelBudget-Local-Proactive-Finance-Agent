from __future__ import annotations

from uuid import UUID

import streamlit as st

from ui.components import render_section_header
from ui.state import UIServices, db_transaction


def render(user_id: UUID, services: UIServices) -> None:
    render_section_header("Settings / Debug", "Lightweight local diagnostics for the UI layer.")

    st.write("Active user")
    st.code(str(user_id))

    st.write("Runtime configuration")
    st.json(
        {
            "sentinel_env": services.settings.sentinel_env,
            "log_level": services.settings.log_level,
            "postgres_host": services.settings.postgres_host,
            "postgres_port": services.settings.postgres_port,
            "postgres_db": services.settings.postgres_db,
            "ollama_base_url": str(services.settings.ollama_base_url),
            "ollama_chat_model": services.settings.ollama_chat_model,
            "memory_embedding_model": services.settings.memory_embedding_model,
            "memory_embedding_dim": services.settings.memory_embedding_dim,
        }
    )

    try:
        with db_transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM accounts WHERE user_id = %s) AS account_count,
                        (SELECT COUNT(*) FROM ledger l
                          INNER JOIN accounts a ON a.account_id = l.account_id
                          WHERE a.user_id = %s) AS transaction_count,
                        (SELECT COUNT(*) FROM insights WHERE user_id = %s) AS insight_count,
                        (SELECT COUNT(*) FROM semantic_memory WHERE user_id = %s) AS memory_count;
                    """,
                    (user_id, user_id, user_id, user_id),
                )
                row = cur.fetchone()
    except Exception as exc:
        st.warning(f"Unable to load debug counts: {exc}")
        return

    if row is None:
        st.info("No diagnostic rows returned.")
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Accounts", int(row[0]))
    col2.metric("Transactions", int(row[1]))
    col3.metric("Insights", int(row[2]))
    col4.metric("Semantic Memory", int(row[3]))
