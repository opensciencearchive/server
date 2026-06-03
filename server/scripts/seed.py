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

SCHEMA_ID = "seed-sample-survey"
SCHEMA_VERSION = "1.0.0"
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
        # Schema — keyed on (id, version)
        row = await conn.execute(
            text("SELECT id FROM schemas WHERE id = :id AND version = :version"),
            {"id": SCHEMA_ID, "version": SCHEMA_VERSION},
        )
        if row.scalar_one_or_none() is None:
            await conn.execute(
                text(
                    "INSERT INTO schemas (id, version, title, fields, created_at) "
                    "VALUES (:id, :version, :title, :fields, :created_at)"
                ),
                {
                    "id": SCHEMA_ID,
                    "version": SCHEMA_VERSION,
                    "title": "Sample Survey",
                    "fields": json.dumps(SCHEMA_FIELDS),
                    "created_at": now,
                },
            )
            print(f"seed: created schema {SCHEMA_ID}@{SCHEMA_VERSION}")
        else:
            print(f"seed: schema already exists {SCHEMA_ID}@{SCHEMA_VERSION}")

        # Convention — references schemas by (schema_id, schema_version);
        # `hooks` replaces the old `validator_refs` column; `source` is nullable.
        row = await conn.execute(
            text("SELECT srn FROM conventions WHERE srn = :srn"),
            {"srn": CONVENTION_SRN},
        )
        if row.scalar_one_or_none() is None:
            await conn.execute(
                text(
                    "INSERT INTO conventions "
                    "(srn, title, description, schema_id, schema_version, "
                    "file_requirements, hooks, source, created_at) "
                    "VALUES (:srn, :title, :description, :schema_id, :schema_version, "
                    ":file_requirements, :hooks, NULL, :created_at)"
                ),
                {
                    "srn": CONVENTION_SRN,
                    "title": "Sample Survey",
                    "description": (
                        "A simple convention for testing the deposition workflow. "
                        "Upload any CSV/TSV/JSON file with basic metadata."
                    ),
                    "schema_id": SCHEMA_ID,
                    "schema_version": SCHEMA_VERSION,
                    "file_requirements": json.dumps(FILE_REQUIREMENTS),
                    "hooks": json.dumps([]),
                    "created_at": now,
                },
            )
            print(f"seed: created convention {CONVENTION_SRN}")
        else:
            print(f"seed: convention already exists {CONVENTION_SRN}")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(seed())
