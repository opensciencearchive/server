from sqlalchemy import (
    Column,
    MetaData,
    String,
    Table,
    Index,
)
from sqlalchemy.dialects.postgresql import TIMESTAMP, JSONB

# Metadata object for all tables
metadata = MetaData()

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
