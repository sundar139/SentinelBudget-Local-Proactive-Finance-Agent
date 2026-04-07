"""Phase 6 conversation history storage.

Revision ID: 20260406_0003
Revises: 20260406_0002
Create Date: 2026-04-06
"""

from __future__ import annotations

from alembic import op

revision = "20260406_0003"
down_revision = "20260406_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS conversation_history (
            id BIGSERIAL PRIMARY KEY,
            session_id UUID NOT NULL,
            user_id UUID NOT NULL REFERENCES users(user_id),
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT chk_conversation_history_role
                CHECK (role IN ('system', 'user', 'assistant', 'tool'))
        );
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_conversation_history_session_created
            ON conversation_history(session_id, created_at DESC, id DESC);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_conversation_history_user_session_created
            ON conversation_history(user_id, session_id, created_at DESC, id DESC);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_conversation_history_user_session_created;")
    op.execute("DROP INDEX IF EXISTS idx_conversation_history_session_created;")
    op.execute("DROP TABLE IF EXISTS conversation_history CASCADE;")
