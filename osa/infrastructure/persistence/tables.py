from sqlalchemy import (
    Column,
    Index,
    MetaData,
    String,
    Table,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP

# Metadata object for all tables
metadata = MetaData()

# ============================================================================
# DEPOSITIONS TABLE
# ============================================================================
depositions_table = Table(
    "depositions",
    metadata,
    Column("srn", String, primary_key=True),
    Column("profile_srn", String, nullable=False),
    Column("status", String, nullable=False),
    Column("payload", JSONB, nullable=False, server_default="{}"),
    Column("files", JSONB, nullable=False, server_default="[]"),
    Column("record_id", String, nullable=True),
    Column(
        "created_at", TIMESTAMP(timezone=True), nullable=False, server_default="NOW()"
    ),
    Column(
        "updated_at", TIMESTAMP(timezone=True), nullable=False, server_default="NOW()"
    ),
)

Index("idx_depositions_record_id", depositions_table.c.record_id)

# ============================================================================
# SHADOW REQUESTS TABLE
# ============================================================================
shadow_requests_table = Table(
    "shadow_requests",
    metadata,
    Column("id", String, primary_key=True),  # ShadowId (ULID/UUID)
    Column("source_url", String, nullable=False),
    Column(
        "status", String, nullable=False
    ),  # Enum stored as string or we can use Enum type
    Column("profile_srn", String, nullable=False),
    Column("deposition_id", String, nullable=True),  # Nullable initially
    Column(
        "created_at", TIMESTAMP(timezone=True), nullable=False, server_default="NOW()"
    ),
    Column(
        "updated_at", TIMESTAMP(timezone=True), nullable=False, server_default="NOW()"
    ),
)

Index("idx_shadow_requests_deposition_id", shadow_requests_table.c.deposition_id)


# ============================================================================
# SHADOW REPORTS TABLE
# ============================================================================
shadow_reports_table = Table(
    "shadow_reports",
    metadata,
    Column("shadow_id", String, primary_key=True),  # Same ID as request
    Column("source_domain", String, nullable=False),
    Column("validation_summary", JSONB, nullable=False),
    Column("score", String, nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
)


# ============================================================================
# TRAITS TABLE
# ============================================================================
traits_table = Table(
    "traits",
    metadata,
    Column("srn", String, primary_key=True),
    Column("slug", String, nullable=False),
    Column("name", String, nullable=False),
    Column("description", String, nullable=False),
    Column("validator", JSONB, nullable=False),  # Validator as JSON (ref + limits)
    Column("status", String, nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
)

Index("idx_traits_slug", traits_table.c.slug)
Index("idx_traits_status", traits_table.c.status)


# ============================================================================
# VALIDATION RUNS TABLE
# ============================================================================
validation_runs_table = Table(
    "validation_runs",
    metadata,
    Column("srn", String, primary_key=True),
    Column("trait_srns", JSONB, nullable=False, server_default="[]"),
    Column("status", String, nullable=False),
    Column("results", JSONB, nullable=False, server_default="[]"),
    Column("started_at", TIMESTAMP(timezone=True), nullable=True),
    Column("completed_at", TIMESTAMP(timezone=True), nullable=True),
    Column("expires_at", TIMESTAMP(timezone=True), nullable=True),
)

Index("idx_validation_runs_expires_at", validation_runs_table.c.expires_at)
