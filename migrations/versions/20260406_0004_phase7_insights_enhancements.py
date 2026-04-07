"""Phase 7 insights evidence and dedup enhancements.

Revision ID: 20260406_0004
Revises: 20260406_0003
Create Date: 2026-04-06
"""

from __future__ import annotations

from alembic import op

revision = "20260406_0004"
down_revision = "20260406_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE insights
        ADD COLUMN IF NOT EXISTS details JSONB NOT NULL DEFAULT '{}'::jsonb;
        """
    )
    op.execute(
        """
        ALTER TABLE insights
        ADD COLUMN IF NOT EXISTS fingerprint TEXT NULL;
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_insights_user_unread_created
            ON insights(user_id, is_read, created_at DESC);
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_insights_user_fingerprint_unread
            ON insights(user_id, fingerprint)
            WHERE is_read = false AND fingerprint IS NOT NULL;
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_insights_user_fingerprint_unread;")
    op.execute("DROP INDEX IF EXISTS idx_insights_user_unread_created;")
    op.execute("ALTER TABLE insights DROP COLUMN IF EXISTS fingerprint;")
    op.execute("ALTER TABLE insights DROP COLUMN IF EXISTS details;")
