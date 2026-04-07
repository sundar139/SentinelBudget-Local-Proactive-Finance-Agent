"""Phase 10 align semantic memory vector dimension to 768.

Revision ID: 20260407_0005
Revises: 20260406_0004
Create Date: 2026-04-07
"""

from __future__ import annotations

from alembic import op

revision = "20260407_0005"
down_revision = "20260406_0004"
branch_labels = None
depends_on = None

TARGET_DIMENSION = 768


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector;")

    op.execute(
        f"""
        DO $$
        DECLARE
            current_type text;
            current_dim integer;
        BEGIN
            IF to_regclass('public.semantic_memory') IS NULL THEN
                RETURN;
            END IF;

            SELECT format_type(a.atttypid, a.atttypmod)
              INTO current_type
              FROM pg_attribute a
              INNER JOIN pg_class c ON c.oid = a.attrelid
              INNER JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'public'
               AND c.relname = 'semantic_memory'
               AND a.attname = 'embedding'
               AND NOT a.attisdropped
             LIMIT 1;

            IF current_type IS NULL THEN
                RETURN;
            END IF;

            current_dim = substring(current_type from 'vector\\(([0-9]+)\\)')::integer;

            IF current_dim IS NULL THEN
                RAISE EXCEPTION
                    'Unable to determine semantic_memory.embedding vector dimension from type %',
                    current_type;
            END IF;

            IF current_dim = {TARGET_DIMENSION} THEN
                RETURN;
            END IF;

            IF current_dim > {TARGET_DIMENSION} THEN
                RAISE EXCEPTION
                    'semantic_memory.embedding dimension % cannot be reduced to % automatically',
                    current_dim, {TARGET_DIMENSION};
            END IF;

            DROP INDEX IF EXISTS idx_semantic_memory_embedding;

            ALTER TABLE semantic_memory
            ALTER COLUMN embedding TYPE vector({TARGET_DIMENSION})
            USING (
                regexp_replace(
                    embedding::text,
                    '\\]$',
                    repeat(',0', {TARGET_DIMENSION} - current_dim) || ']'
                )
            )::vector({TARGET_DIMENSION});

            CREATE INDEX IF NOT EXISTS idx_semantic_memory_embedding
                ON semantic_memory USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100);
        END
        $$;
        """
    )


def downgrade() -> None:
    # Irreversible without truncating vectors and potentially losing data.
    pass
