"""Seed the database with sample data for local development.

Standalone script — no osa package imports required.
Uses SQLAlchemy directly with $OSA_DATABASE__URL from the environment.
Idempotent: safe to run on every startup.
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# ---------------------------------------------------------------------------
# Seed data
# ---------------------------------------------------------------------------

SCHEMA_SRN = "urn:osa:localhost:schema:seed-sample-survey@1.0.0"
CONVENTION_SRN = "urn:osa:localhost:conv:seed-sample-survey@1.0.0"

SCHEMA_FIELDS = [
    {
        "name": "title",
        "type": "text",
        "required": True,
        "cardinality": "exactly_one",
        "description": "Title of the submission",
        "constraints": {"type": "text", "min_length": 1, "max_length": 200},
    },
    {
        "name": "author",
        "type": "text",
        "required": True,
        "cardinality": "exactly_one",
        "description": "Name of the primary author",
        "constraints": {"type": "text", "min_length": 1, "max_length": 100},
    },
    {
        "name": "description",
        "type": "text",
        "required": True,
        "cardinality": "exactly_one",
        "description": "A brief description of the data",
        "constraints": {"type": "text", "max_length": 2000},
    },
    {
        "name": "date",
        "type": "date",
        "required": True,
        "cardinality": "exactly_one",
        "description": "Date of data collection (YYYY-MM-DD)",
        "constraints": None,
    },
    {
        "name": "sample_count",
        "type": "number",
        "required": True,
        "cardinality": "exactly_one",
        "description": "Number of samples in the dataset",
        "constraints": {"type": "number", "min_value": 1, "integer_only": True},
    },
    {
        "name": "notes",
        "type": "text",
        "required": False,
        "cardinality": "exactly_one",
        "description": "Any additional notes or comments",
        "constraints": None,
    },
]

FILE_REQUIREMENTS = {
    "accepted_types": [".csv", ".tsv", ".txt", ".json", ".xlsx"],
    "min_count": 1,
    "max_count": 20,
    "max_file_size": 104_857_600,
}


async def seed() -> None:
    db_url = os.environ.get("OSA_DATABASE__URL")
    if not db_url:
        print("seed: OSA_DATABASE__URL not set, skipping", file=sys.stderr)
        sys.exit(1)

    engine = create_async_engine(db_url)
    now = datetime.now(timezone.utc)

    async with engine.begin() as conn:
        # Schema
        row = await conn.execute(
            text("SELECT srn FROM schemas WHERE srn = :srn"),
            {"srn": SCHEMA_SRN},
        )
        if row.scalar_one_or_none() is None:
            await conn.execute(
                text(
                    "INSERT INTO schemas (srn, title, fields, created_at) "
                    "VALUES (:srn, :title, :fields, :created_at)"
                ),
                {
                    "srn": SCHEMA_SRN,
                    "title": "Sample Survey",
                    "fields": json.dumps(SCHEMA_FIELDS),
                    "created_at": now,
                },
            )
            print(f"seed: created schema {SCHEMA_SRN}")
        else:
            print(f"seed: schema already exists {SCHEMA_SRN}")

        # Convention
        row = await conn.execute(
            text("SELECT srn FROM conventions WHERE srn = :srn"),
            {"srn": CONVENTION_SRN},
        )
        if row.scalar_one_or_none() is None:
            await conn.execute(
                text(
                    "INSERT INTO conventions (srn, title, description, schema_srn, file_requirements, validator_refs, created_at) "
                    "VALUES (:srn, :title, :description, :schema_srn, :file_requirements, :validator_refs, :created_at)"
                ),
                {
                    "srn": CONVENTION_SRN,
                    "title": "Sample Survey",
                    "description": (
                        "A simple convention for testing the deposition workflow. "
                        "Upload any CSV/TSV/JSON file with basic metadata."
                    ),
                    "schema_srn": SCHEMA_SRN,
                    "file_requirements": json.dumps(FILE_REQUIREMENTS),
                    "validator_refs": json.dumps([]),
                    "created_at": now,
                },
            )
            print(f"seed: created convention {CONVENTION_SRN}")
        else:
            print(f"seed: convention already exists {CONVENTION_SRN}")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(seed())
