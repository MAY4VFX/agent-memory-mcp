"""SQLAlchemy Core table definitions."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR, UUID

metadata = MetaData()

users = Table(
    "users",
    metadata,
    Column("telegram_id", BigInteger, primary_key=True),
    Column("username", String(255)),
    Column("language", String(8), server_default="ru"),
    Column("detail_level", String(16), server_default="normal"),
    Column("active_domain_id", UUID(as_uuid=True)),
    Column("active_conversation_id", UUID(as_uuid=True)),
    Column("active_scope_type", String(16), server_default="domain"),
    Column("active_group_id", UUID(as_uuid=True)),
    Column("created_at", DateTime(timezone=True), server_default=text("now()")),
)

domains = Table(
    "domains",
    metadata,
    Column(
        "id",
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column(
        "owner_id",
        BigInteger,
        ForeignKey("users.telegram_id"),
        nullable=False,
    ),
    Column("channel_id", BigInteger, nullable=False),
    Column("channel_username", String(128)),
    Column("channel_name", String(255)),
    Column("emoji", String(8), server_default="'\U0001f4da'"),
    Column("display_name", String(255)),
    Column("domain_type", String(64)),
    Column("sync_depth", String(16), nullable=False),
    Column("sync_frequency_minutes", Integer, nullable=False),
    Column("sync_from_date", DateTime(timezone=True)),
    Column("last_synced_at", DateTime(timezone=True)),
    Column("last_synced_message_id", BigInteger, server_default="0"),
    Column("next_sync_at", DateTime(timezone=True)),
    Column("message_count", Integer, server_default="0"),
    Column("entity_count", Integer, server_default="0"),
    Column("relation_count", Integer, server_default="0"),
    Column("is_active", Boolean, server_default="true"),
    Column("pinned", Boolean, server_default="false"),
    Column("created_at", DateTime(timezone=True), server_default=text("now()")),
    UniqueConstraint("owner_id", "channel_id"),
)

# Deferred FK: users.active_domain_id -> domains.id
# Applied via Alembic migration (ALTER TABLE).

channel_schemas = Table(
    "channel_schemas",
    metadata,
    Column(
        "id",
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column(
        "domain_id",
        UUID(as_uuid=True),
        ForeignKey("domains.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("version", Integer, nullable=False, server_default="1"),
    Column("schema_json", JSONB, nullable=False),
    Column("detected_domain", String(64)),
    Column("entity_types", JSONB),
    Column("relation_types", JSONB),
    Column("is_active", Boolean, server_default="true"),
    Column("langfuse_trace_id", String(128)),
    Column("created_at", DateTime(timezone=True), server_default=text("now()")),
    UniqueConstraint("domain_id", "version"),
)

messages = Table(
    "messages",
    metadata,
    Column(
        "id",
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column(
        "domain_id",
        UUID(as_uuid=True),
        ForeignKey("domains.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("telegram_msg_id", BigInteger, nullable=False),
    Column("reply_to_msg_id", BigInteger),
    Column("topic_id", BigInteger),
    Column("thread_id", UUID(as_uuid=True)),
    Column("sender_id", BigInteger),
    Column("sender_name", String(255)),
    Column("content", Text),
    Column("content_type", String(32), server_default="text"),
    Column("raw_json", JSONB),
    Column("language", String(8)),
    Column("hashtags", JSONB),
    Column("content_tsv", TSVECTOR, nullable=True),
    Column("is_noise", Boolean, server_default="false"),
    Column("msg_date", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), server_default=text("now()")),
    UniqueConstraint("domain_id", "telegram_msg_id"),
    Index("idx_messages_domain_date", "domain_id", "msg_date", postgresql_using="btree"),
)

# Partial index for reply lookups
Index(
    "idx_messages_reply",
    messages.c.domain_id,
    messages.c.reply_to_msg_id,
    postgresql_where=messages.c.reply_to_msg_id.isnot(None),
)

threads = Table(
    "threads",
    metadata,
    Column(
        "id",
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column(
        "domain_id",
        UUID(as_uuid=True),
        ForeignKey("domains.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("root_message_id", UUID(as_uuid=True), ForeignKey("messages.id")),
    Column("msg_count", Integer, server_default="1"),
    Column("combined_text", Text),
    Column("first_msg_date", DateTime(timezone=True)),
    Column("last_msg_date", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), server_default=text("now()")),
)

conversations = Table(
    "conversations",
    metadata,
    Column(
        "id",
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column("user_id", BigInteger, ForeignKey("users.telegram_id"), nullable=False),
    Column("domain_id", UUID(as_uuid=True), ForeignKey("domains.id", ondelete="SET NULL")),
    Column("title", String(255)),
    Column("is_active", Boolean, server_default="true"),
    Column("message_count", Integer, server_default="0"),
    Column("scope_type", String(16), server_default="domain"),
    Column("group_id", UUID(as_uuid=True)),
    Column("created_at", DateTime(timezone=True), server_default=text("now()")),
    Column("updated_at", DateTime(timezone=True), server_default=text("now()")),
)

conversation_messages = Table(
    "conversation_messages",
    metadata,
    Column(
        "id",
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column(
        "conversation_id",
        UUID(as_uuid=True),
        ForeignKey("conversations.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("role", String(16), nullable=False),
    Column("content", Text, nullable=False),
    Column("context_payload_id", UUID(as_uuid=True)),
    Column("langfuse_trace_id", String(128)),
    Column("token_count", Integer, server_default="0"),
    Column("created_at", DateTime(timezone=True), server_default=text("now()")),
    Index("idx_conv_messages_conv_id", "conversation_id", "created_at"),
)

context_payloads = Table(
    "context_payloads",
    metadata,
    Column(
        "id",
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column(
        "conversation_id",
        UUID(as_uuid=True),
        ForeignKey("conversations.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("query_text", Text, nullable=False),
    Column("payload_json", JSONB, nullable=False),
    Column("token_count", Integer, server_default="0"),
    Column("chunks_count", Integer, server_default="0"),
    Column("graph_entities_count", Integer, server_default="0"),
    Column("langfuse_trace_id", String(128)),
    Column("created_at", DateTime(timezone=True), server_default=text("now()")),
)

feedback = Table(
    "feedback",
    metadata,
    Column(
        "id",
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column(
        "message_id",
        UUID(as_uuid=True),
        ForeignKey("conversation_messages.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("user_id", BigInteger, ForeignKey("users.telegram_id"), nullable=False),
    Column("score", Integer, nullable=False),
    Column("created_at", DateTime(timezone=True), server_default=text("now()")),
    UniqueConstraint("message_id", "user_id"),
)

hashtag_summaries = Table(
    "hashtag_summaries",
    metadata,
    Column(
        "id",
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column(
        "domain_id",
        UUID(as_uuid=True),
        ForeignKey("domains.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("hashtag", String(255), nullable=False),
    Column("post_count", Integer, server_default="0"),
    Column("summary", Text),
    Column("is_stale", Boolean, server_default="false"),
    Column("posts_since_update", Integer, server_default="0"),
    Column("generated_at", DateTime(timezone=True), server_default=text("now()")),
    Column("created_at", DateTime(timezone=True), server_default=text("now()")),
    UniqueConstraint("domain_id", "hashtag"),
    Index("idx_hashtag_summaries_domain", "domain_id"),
)

domain_groups = Table(
    "domain_groups",
    metadata,
    Column(
        "id",
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column("owner_id", BigInteger, ForeignKey("users.telegram_id"), nullable=False),
    Column("name", String(255), nullable=False),
    Column("emoji", String(8), server_default="'\U0001f4c1'"),
    Column("tg_folder_id", Integer),
    Column("sync_depth", String(8)),
    Column("is_active", Boolean, server_default="true"),
    Column("created_at", DateTime(timezone=True), server_default=text("now()")),
    UniqueConstraint("owner_id", "name"),
)

domain_group_members = Table(
    "domain_group_members",
    metadata,
    Column(
        "group_id",
        UUID(as_uuid=True),
        ForeignKey("domain_groups.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "domain_id",
        UUID(as_uuid=True),
        ForeignKey("domains.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("added_at", DateTime(timezone=True), server_default=text("now()")),
    PrimaryKeyConstraint("group_id", "domain_id"),
)

digest_configs = Table(
    "digest_configs",
    metadata,
    Column(
        "id",
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column("user_id", BigInteger, ForeignKey("users.telegram_id"), nullable=False),
    Column("name", String(255), server_default="'Daily Digest'"),
    Column("scope_type", String(16), server_default="all"),
    Column("scope_id", UUID(as_uuid=True)),
    Column("send_hour_utc", Integer, server_default="8"),
    Column("is_active", Boolean, server_default="true"),
    Column("last_sent_at", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), server_default=text("now()")),
    UniqueConstraint("user_id", "name"),
)

digest_runs = Table(
    "digest_runs",
    metadata,
    Column(
        "id",
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column(
        "config_id",
        UUID(as_uuid=True),
        ForeignKey("digest_configs.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("user_id", BigInteger),
    Column("status", String(16), server_default="pending"),
    Column("domain_count", Integer),
    Column("message_count", Integer),
    Column("digest_text", Text),
    Column("langfuse_trace_id", String(128)),
    Column("error_message", Text),
    Column("started_at", DateTime(timezone=True)),
    Column("completed_at", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), server_default=text("now()")),
)

sync_jobs = Table(
    "sync_jobs",
    metadata,
    Column(
        "id",
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column(
        "domain_id",
        UUID(as_uuid=True),
        ForeignKey("domains.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("job_type", String(16), nullable=False),
    Column("status", String(16), server_default="pending"),
    Column("messages_fetched", Integer, server_default="0"),
    Column("messages_filtered", Integer, server_default="0"),
    Column("messages_processed", Integer, server_default="0"),
    Column("messages_total", Integer),
    Column("entities_extracted", Integer, server_default="0"),
    Column("error_message", Text),
    Column("langfuse_trace_id", String(128)),
    Column("started_at", DateTime(timezone=True)),
    Column("completed_at", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), server_default=text("now()")),
)
