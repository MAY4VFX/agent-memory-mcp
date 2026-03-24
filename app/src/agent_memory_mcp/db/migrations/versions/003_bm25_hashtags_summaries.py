"""BM25 index, hashtags backfill, hashtag_summaries table.

Revision ID: 003
Revises: 002
Create Date: 2025-08-22
"""

from typing import Sequence, Union

from alembic import op

revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- ParadeDB BM25 index with Russian stemmer (if pg_search available) ---
    op.execute("""
        DO $$
        BEGIN
            CREATE EXTENSION IF NOT EXISTS pg_search;

            CREATE INDEX IF NOT EXISTS idx_messages_bm25 ON messages
                USING bm25 (id, (content::pdb.simple('stemmer=russian')))
                WITH (key_field = 'id');
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE 'pg_search not available: %', SQLERRM;
        END $$;
    """)

    # --- tsvector column + GIN index (always, as fallback) ---
    op.execute("""
        ALTER TABLE messages ADD COLUMN IF NOT EXISTS content_tsv TSVECTOR;
    """)
    op.execute("""
        UPDATE messages SET content_tsv = to_tsvector('russian', COALESCE(content, ''))
            WHERE content IS NOT NULL AND content_tsv IS NULL;
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_messages_content_tsv
            ON messages USING GIN(content_tsv);
    """)
    op.execute("""
        CREATE OR REPLACE FUNCTION messages_tsv_trigger() RETURNS trigger AS $t$
        BEGIN
            NEW.content_tsv := to_tsvector('russian', COALESCE(NEW.content, ''));
            RETURN NEW;
        END;
        $t$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS trg_messages_tsv ON messages;
        CREATE TRIGGER trg_messages_tsv
            BEFORE INSERT OR UPDATE OF content ON messages
            FOR EACH ROW EXECUTE FUNCTION messages_tsv_trigger();
    """)

    # --- Backfill hashtags ---
    op.execute("""
        UPDATE messages SET hashtags = sub.tags
        FROM (
            SELECT id, jsonb_agg(m[1]) AS tags
            FROM messages, regexp_matches(content, '#(\\w+)', 'g') AS m
            WHERE content LIKE '%#%' AND hashtags IS NULL
            GROUP BY id
        ) AS sub
        WHERE messages.id = sub.id;
    """)

    # --- GIN index on hashtags ---
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_messages_hashtags
            ON messages USING GIN(hashtags);
    """)

    # --- Hashtag summaries table ---
    op.execute("""
        CREATE TABLE hashtag_summaries (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            domain_id       UUID NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
            hashtag         VARCHAR(255) NOT NULL,
            post_count      INTEGER DEFAULT 0,
            summary         TEXT,
            is_stale        BOOLEAN DEFAULT false,
            posts_since_update INTEGER DEFAULT 0,
            generated_at    TIMESTAMPTZ DEFAULT now(),
            created_at      TIMESTAMPTZ DEFAULT now(),
            UNIQUE(domain_id, hashtag)
        );
    """)

    op.execute("""
        CREATE INDEX idx_hashtag_summaries_domain
            ON hashtag_summaries(domain_id);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS hashtag_summaries CASCADE;")
    op.execute("DROP INDEX IF EXISTS idx_messages_hashtags;")

    # Try dropping ParadeDB BM25 index
    op.execute("""
        DO $$
        BEGIN
            DROP INDEX IF EXISTS idx_messages_bm25;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END $$;
    """)
    # Drop tsvector artifacts (if they exist)
    op.execute("DROP TRIGGER IF EXISTS trg_messages_tsv ON messages;")
    op.execute("DROP FUNCTION IF EXISTS messages_tsv_trigger();")
    op.execute("DROP INDEX IF EXISTS idx_messages_content_tsv;")
    op.execute("ALTER TABLE messages DROP COLUMN IF EXISTS content_tsv;")
