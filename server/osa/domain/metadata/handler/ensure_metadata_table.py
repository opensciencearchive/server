"""EnsureMetadataTable — creates/evolves the typed metadata table on ConventionRegistered."""

from __future__ import annotations

import logging

from osa.domain.deposition.event.convention_registered import ConventionRegistered
from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.metadata.service.metadata import MetadataService
from osa.domain.semantics.port.schema_repository import SchemaRepository
from osa.domain.shared.error import DomainError, NotFoundError
from osa.domain.shared.event import EventHandler

logger = logging.getLogger(__name__)


class EnsureMetadataTable(EventHandler[ConventionRegistered]):
    """Reacts to ConventionRegistered, creates/evolves the schema's metadata table.

    Idempotent and schema-keyed: two conventions against the same
    ``(schema_identity, schema_major)`` share one table. Additive minor/patch
    bumps trigger ALTER ADD COLUMN.
    """

    metadata_service: MetadataService
    schema_repo: SchemaRepository
    convention_repo: ConventionRepository

    async def handle(self, event: ConventionRegistered) -> None:
        convention = await self.convention_repo.get(event.convention_srn)
        if convention is None:
            raise NotFoundError(f"Convention not found: {event.convention_srn}")

        schema = await self.schema_repo.get(event.schema_srn)
        if schema is None:
            raise NotFoundError(f"Schema not found: {event.schema_srn}")

        try:
            await self.metadata_service.ensure_table(
                schema_srn=event.schema_srn,
                schema_title=schema.title,
                fields=event.schema_fields,
            )
        except DomainError:
            logger.exception(
                "EnsureMetadataTable failed: convention=%s schema=%s",
                event.convention_srn,
                event.schema_srn,
            )
            raise
