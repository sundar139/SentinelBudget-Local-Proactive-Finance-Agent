"""Phase 5 semantic memory schema.

Revision ID: 20260406_0002
Revises: 20260406_0001
Create Date: 2026-04-06
"""

from __future__ import annotations

from alembic import op

revision = "20260406_0002"
down_revision = "20260406_0001"
branch_labels = None
depends_on = None

EMBEDDING_DIMENSION = 768


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector;")

    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS semantic_memory (
            id BIGSERIAL PRIMARY KEY,
            user_id UUID NOT NULL REFERENCES users(user_id),
            embedding vector({EMBEDDING_DIMENSION}) NOT NULL,
            kind TEXT NOT NULL,
            text TEXT NOT NULL,
            metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_semantic_memory_user_kind_created_at
            ON semantic_memory(user_id, kind, created_at DESC);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_semantic_memory_embedding
            ON semantic_memory USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 100);
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_semantic_memory_source
            ON semantic_memory(user_id, kind, ((metadata->>'source')), ((metadata->>'source_id')))
            WHERE (metadata ? 'source') AND (metadata ? 'source_id');
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_semantic_memory_source;")
    op.execute("DROP INDEX IF EXISTS idx_semantic_memory_embedding;")
    op.execute("DROP INDEX IF EXISTS idx_semantic_memory_user_kind_created_at;")
    op.execute("DROP TABLE IF EXISTS semantic_memory CASCADE;")
