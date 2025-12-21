"""SQLAlchemy table definitions - dialect-agnostic (works with SQLite and PostgreSQL)."""

from sqlalchemy import (
    Column,
    DateTime,
    Index,
    MetaData,
    String,
    Table,
    Text,
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
    Column("status", String(32), nullable=False),  # DepositionStatus as string
    Column("metadata", JSON, nullable=False),
    Column("provenance", JSON, nullable=False),
    Column("files", JSON, nullable=False),
    Column("record_id", String, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

Index("idx_depositions_record_id", depositions_table.c.record_id)


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
    Column("delivery_status", String(32), nullable=False),  # pending, delivered, failed
    Column("delivered_at", DateTime(timezone=True), nullable=True),
    Column("delivery_error", Text, nullable=True),
)

Index("idx_events_type_created", events_table.c.event_type, events_table.c.created_at.desc())
Index("idx_events_delivery_status", events_table.c.delivery_status)
