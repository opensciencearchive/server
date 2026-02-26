from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import uuid4

from osa.domain.auth.model.value import UserId
from osa.domain.deposition.event.created import DepositionCreatedEvent
from osa.domain.deposition.event.file_deleted import FileDeletedEvent
from osa.domain.deposition.event.file_uploaded import FileUploadedEvent
from osa.domain.deposition.event.metadata_updated import MetadataUpdatedEvent
from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.deposition.model.value import DepositionFile
from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.deposition.port.repository import DepositionRepository
from osa.domain.deposition.port.storage import FileStoragePort
from osa.domain.shared.error import NotFoundError, ValidationError
from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.model.hook_snapshot import HookSnapshot
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN, Domain, LocalId
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.service import Service


class DepositionService(Service):
    deposition_repo: DepositionRepository
    convention_repo: ConventionRepository
    file_storage: FileStoragePort
    outbox: Outbox
    node_domain: Domain

    async def create(
        self,
        convention_srn: ConventionSRN,
        owner_id: UserId,
    ) -> Deposition:
        convention = await self.convention_repo.get(convention_srn)
        if convention is None:
            raise NotFoundError(f"Convention not found: {convention_srn}")

        now = datetime.now(UTC)
        srn = DepositionSRN(
            domain=self.node_domain,
            id=LocalId(str(uuid4())[:20]),
        )
        deposition = Deposition(
            srn=srn,
            convention_srn=convention_srn,
            owner_id=owner_id,
            created_at=now,
            updated_at=now,
        )
        await self.deposition_repo.save(deposition)

        event = DepositionCreatedEvent(
            id=EventId(uuid4()),
            deposition_id=srn,
            convention_srn=convention_srn,
            owner_id=owner_id,
        )
        await self.outbox.append(event)
        return deposition

    async def get(self, srn: DepositionSRN) -> Deposition:
        dep = await self.deposition_repo.get(srn)
        if dep is None:
            raise NotFoundError(f"Deposition not found: {srn}")
        return dep

    async def update_metadata(
        self,
        srn: DepositionSRN,
        metadata: dict,
    ) -> Deposition:
        dep = await self.get(srn)
        dep.update_metadata(metadata)
        await self.deposition_repo.save(dep)

        event = MetadataUpdatedEvent(
            id=EventId(uuid4()),
            deposition_id=srn,
            metadata=metadata,
        )
        await self.outbox.append(event)
        return dep

    async def upload_file(
        self,
        srn: DepositionSRN,
        filename: str,
        content: bytes,
        size: int,
    ) -> Deposition:
        dep = await self.get(srn)
        convention = await self.convention_repo.get(dep.convention_srn)
        if convention is None:
            raise NotFoundError(f"Convention not found: {dep.convention_srn}")

        reqs = convention.file_requirements

        # Validate file type
        ext = _get_extension(filename)
        if reqs.accepted_types and ext not in reqs.accepted_types:
            raise ValidationError(f"File type '{ext}' not accepted. Allowed: {reqs.accepted_types}")

        # Validate file size
        if size > reqs.max_file_size:
            raise ValidationError(f"File size {size} exceeds maximum {reqs.max_file_size}")

        # Validate max count
        if len(dep.files) >= reqs.max_count:
            raise ValidationError(
                f"Maximum {reqs.max_count} files allowed, already have {len(dep.files)}"
            )

        # Store the file and get back the DepositionFile VO
        saved_file = await self.file_storage.save_file(srn, filename, content, size)
        dep.add_file(saved_file)
        await self.deposition_repo.save(dep)

        event = FileUploadedEvent(
            id=EventId(uuid4()),
            deposition_id=srn,
            filename=saved_file.name,
            size=saved_file.size,
            checksum=saved_file.checksum,
        )
        await self.outbox.append(event)
        return dep

    async def delete_file(
        self,
        srn: DepositionSRN,
        filename: str,
    ) -> Deposition:
        dep = await self.get(srn)
        dep.remove_file(filename)
        await self.file_storage.delete_file(srn, filename)
        await self.deposition_repo.save(dep)

        event = FileDeletedEvent(
            id=EventId(uuid4()),
            deposition_id=srn,
            filename=filename,
        )
        await self.outbox.append(event)
        return dep

    async def list_depositions(
        self,
        owner_id: UserId | None = None,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> tuple[list[Deposition], int]:
        if owner_id is not None:
            items = await self.deposition_repo.list_by_owner(owner_id, limit=limit, offset=offset)
            total = await self.deposition_repo.count_by_owner(owner_id)
        else:
            items = await self.deposition_repo.list(limit=limit, offset=offset)
            total = await self.deposition_repo.count()
        return items, total

    async def get_file_download(
        self,
        srn: DepositionSRN,
        filename: str,
    ) -> tuple[AsyncIterator[bytes], DepositionFile]:
        """Fetch file stream and metadata in a single deposition lookup."""
        dep = await self.get(srn)
        file_meta = next((f for f in dep.files if f.name == filename), None)
        if file_meta is None:
            raise NotFoundError(f"File '{filename}' not found in deposition")
        stream = await self.file_storage.get_file(srn, filename)
        return stream, file_meta

    async def return_to_draft(self, srn: DepositionSRN) -> Deposition:
        """Transition a deposition back to DRAFT (e.g. after validation failure)."""
        dep = await self.get(srn)
        dep.return_to_draft()
        await self.deposition_repo.save(dep)
        return dep

    async def submit(self, srn: DepositionSRN) -> Deposition:
        dep = await self.get(srn)
        convention = await self.convention_repo.get(dep.convention_srn)
        if convention is None:
            raise NotFoundError(f"Convention not found: {dep.convention_srn}")

        reqs = convention.file_requirements
        if len(dep.files) < reqs.min_count:
            raise ValidationError(
                f"Minimum {reqs.min_count} file(s) required, have {len(dep.files)}"
            )

        dep.submit()
        await self.deposition_repo.save(dep)

        hook_snapshots = _to_hook_snapshots(convention.hooks)
        files_dir = self.file_storage.get_files_dir(dep.srn)

        event = DepositionSubmittedEvent(
            id=EventId(uuid4()),
            deposition_id=srn,
            metadata=dep.metadata,
            convention_srn=dep.convention_srn,
            hooks=hook_snapshots,
            files_dir=str(files_dir),
        )
        await self.outbox.append(event)
        return dep


def _to_hook_snapshots(hooks: list[HookDefinition]) -> list[HookSnapshot]:
    """Convert HookDefinitions to HookSnapshots for event payload."""
    return [
        HookSnapshot(
            name=h.manifest.name,
            image=h.image,
            digest=h.digest,
            features=h.manifest.feature_schema.columns,
            config=h.config or {},
        )
        for h in hooks
    ]


def _get_extension(filename: str) -> str:
    """Extract file extension including dot (e.g., '.csv')."""
    dot_idx = filename.rfind(".")
    if dot_idx == -1:
        return ""
    return filename[dot_idx:].lower()
