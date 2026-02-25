"""SQLAlchemy table definitions - dialect-agnostic (works with SQLite and PostgreSQL)."""

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.types import JSON

# Metadata object for all tables
metadata = MetaData()

# ============================================================================
# DEPOSITIONS TABLE
# ============================================================================
depositions_table = Table(
    "depositions",
    metadata,
    Column("srn", String, primary_key=True),
    Column("convention_srn", String, nullable=False),  # Convention submitted against
    Column("status", String(32), nullable=False),  # DepositionStatus as string
    Column("metadata", JSON, nullable=False),
    Column("files", JSON, nullable=False),
    Column("record_id", String, nullable=True),
    Column("owner_id", String, ForeignKey("users.id"), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

Index("idx_depositions_record_id", depositions_table.c.record_id)
Index("idx_depositions_owner_id", depositions_table.c.owner_id)


# ============================================================================
# VALIDATION RUNS TABLE
# ============================================================================
validation_runs_table = Table(
    "validation_runs",
    metadata,
    Column("srn", String, primary_key=True),
    Column("status", String(32), nullable=False),  # RunStatus as string
    Column("results", JSON, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=True),
    Column("completed_at", DateTime(timezone=True), nullable=True),
    Column("expires_at", DateTime(timezone=True), nullable=True),
)

Index("idx_validation_runs_expires_at", validation_runs_table.c.expires_at)


# ============================================================================
# RECORDS TABLE
# ============================================================================
records_table = Table(
    "records",
    metadata,
    Column("srn", String, primary_key=True),
    Column("deposition_srn", String, nullable=False),
    Column("metadata", JSON, nullable=False),
    Column("indexes", JSON, nullable=False),
    Column("published_at", DateTime(timezone=True), nullable=False),
)

Index("idx_records_deposition_srn", records_table.c.deposition_srn)
Index("idx_records_published_at", records_table.c.published_at)


# ============================================================================
# EVENTS TABLE (Outbox)
# ============================================================================
events_table = Table(
    "events",
    metadata,
    Column("id", String, primary_key=True),
    Column("event_type", String(128), nullable=False),
    Column("payload", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column(
        "delivery_status", String(32), nullable=False
    ),  # pending, claimed, delivered, failed, skipped
    Column("delivered_at", DateTime(timezone=True), nullable=True),
    Column("delivery_error", Text, nullable=True),
    # Pull-based worker columns
    Column("routing_key", String(255), nullable=True),
    Column("retry_count", Integer, nullable=False, default=0),
    Column("claimed_at", DateTime(timezone=True), nullable=True),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

Index(
    "idx_events_type_created",
    events_table.c.event_type,
    events_table.c.created_at.desc(),
)
Index("idx_events_delivery_status", events_table.c.delivery_status)

# Partial index for efficient claiming query
Index(
    "idx_events_claim",
    events_table.c.delivery_status,
    events_table.c.event_type,
    events_table.c.routing_key,
    events_table.c.created_at,
    postgresql_where=text("delivery_status IN ('pending', 'claimed')"),
)

# Partial index for stale claim detection
Index(
    "idx_events_stale_claims",
    events_table.c.claimed_at,
    postgresql_where=text("delivery_status = 'claimed'"),
)

# Partial index for failed event queries
Index(
    "idx_events_failed",
    events_table.c.event_type,
    events_table.c.created_at,
    postgresql_where=text("delivery_status = 'failed'"),
)


# ============================================================================
# USERS TABLE (Authentication)
# ============================================================================
users_table = Table(
    "users",
    metadata,
    Column("id", String, primary_key=True),  # UUID as string
    Column("display_name", String(255), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=True),
)


# ============================================================================
# IDENTITIES TABLE (Authentication)
# ============================================================================
identities_table = Table(
    "identities",
    metadata,
    Column("id", String, primary_key=True),  # UUID as string
    Column("user_id", String, ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
    Column("provider", String(50), nullable=False),  # "orcid", "google", etc.
    Column("external_id", String(255), nullable=False),  # ORCiD ID, Google ID, etc.
    Column("metadata", JSON, nullable=True),  # Provider-specific data (name, email)
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("provider", "external_id", name="uq_identity_provider_external"),
)

Index("ix_identities_user_id", identities_table.c.user_id)


# ============================================================================
# REFRESH TOKENS TABLE (Authentication)
# ============================================================================
refresh_tokens_table = Table(
    "refresh_tokens",
    metadata,
    Column("id", String, primary_key=True),  # UUID as string
    Column("user_id", String, ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
    Column("token_hash", String(64), nullable=False),  # SHA256 hash
    Column("family_id", String, nullable=False),  # UUID - for theft detection
    Column("expires_at", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("revoked_at", DateTime(timezone=True), nullable=True),
)

Index("ix_refresh_tokens_user_id", refresh_tokens_table.c.user_id)
Index("ix_refresh_tokens_token_hash", refresh_tokens_table.c.token_hash)
Index("ix_refresh_tokens_family_id", refresh_tokens_table.c.family_id)


# ============================================================================
# ONTOLOGIES TABLE (Semantics)
# ============================================================================
ontologies_table = Table(
    "ontologies",
    metadata,
    Column("srn", String, primary_key=True),  # Versioned SRN string
    Column("title", String(255), nullable=False),
    Column("description", Text, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
)


# ============================================================================
# ONTOLOGY TERMS TABLE (Semantics)
# ============================================================================
ontology_terms_table = Table(
    "ontology_terms",
    metadata,
    Column("id", String, primary_key=True),  # UUID as string
    Column(
        "ontology_srn",
        String,
        ForeignKey("ontologies.srn", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("term_id", String(255), nullable=False),
    Column("label", String(255), nullable=False),
    Column("synonyms", JSON, nullable=False, default=[]),
    Column("parent_ids", JSON, nullable=False, default=[]),
    Column("definition", Text, nullable=True),
    Column("deprecated", Boolean, nullable=False, default=False),
    UniqueConstraint("ontology_srn", "term_id", name="uq_ontology_term"),
)

Index("idx_ontology_terms_ontology_srn", ontology_terms_table.c.ontology_srn)


# ============================================================================
# SCHEMAS TABLE (Semantics)
# ============================================================================
schemas_table = Table(
    "schemas",
    metadata,
    Column("srn", String, primary_key=True),  # Versioned SRN string
    Column("title", String(255), nullable=False),
    Column("fields", JSON, nullable=False),  # List of FieldDefinition dicts
    Column("created_at", DateTime(timezone=True), nullable=False),
)


# ============================================================================
# CONVENTIONS TABLE (Deposition)
# ============================================================================
conventions_table = Table(
    "conventions",
    metadata,
    Column("srn", String, primary_key=True),  # Versioned SRN string
    Column("title", String(255), nullable=False),
    Column("description", Text, nullable=True),
    Column("schema_srn", String, nullable=False),  # Reference to schemas.srn
    Column("file_requirements", JSON, nullable=False),  # FileRequirements as dict
    Column("hooks", JSON, nullable=False, default=[]),  # List of HookDefinition dicts
    Column("source", JSON, nullable=True),  # SourceDefinition as dict
    Column("created_at", DateTime(timezone=True), nullable=False),
)


# ============================================================================
# FEATURE TABLES CATALOG (Validation)
# ============================================================================
feature_tables_table = Table(
    "feature_tables",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("hook_name", String, nullable=False),
    Column("pg_table", String, nullable=False),
    Column("feature_schema", JSON, nullable=False),
    Column("schema_version", Integer, nullable=False, default=1),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("hook_name", name="uq_feature_tables_hook_name"),
)


# ============================================================================
# ROLE ASSIGNMENTS TABLE (Authorization)
# ============================================================================
role_assignments_table = Table(
    "role_assignments",
    metadata,
    Column("id", String, primary_key=True),  # UUID as string
    Column("user_id", String, ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
    Column("role", String(32), nullable=False),
    Column("assigned_by", String, ForeignKey("users.id"), nullable=False),
    Column("assigned_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("user_id", "role", name="uq_role_assignments_user_role"),
)

Index("ix_role_assignments_user_id", role_assignments_table.c.user_id)
