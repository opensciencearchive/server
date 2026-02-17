"""Unit tests for DepositionService."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.auth.model.value import UserId
from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import (
    DepositionFile,
    DepositionStatus,
    FileRequirements,
)
from osa.domain.deposition.event.created import DepositionCreatedEvent
from osa.domain.deposition.event.file_deleted import FileDeletedEvent
from osa.domain.deposition.event.file_uploaded import FileUploadedEvent
from osa.domain.deposition.event.metadata_updated import MetadataUpdatedEvent
from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.error import NotFoundError, ValidationError
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN, Domain, SchemaSRN


def _make_dep_srn(id: str = "test-dep") -> DepositionSRN:
    return DepositionSRN.parse(f"urn:osa:localhost:dep:{id}")


def _make_conv_srn(id: str = "test-conv", version: str = "1.0.0") -> ConventionSRN:
    return ConventionSRN.parse(f"urn:osa:localhost:conv:{id}@{version}")


def _make_schema_srn(id: str = "test-schema", version: str = "1.0.0") -> SchemaSRN:
    return SchemaSRN.parse(f"urn:osa:localhost:schema:{id}@{version}")


def _make_file_reqs(**overrides) -> FileRequirements:
    defaults = dict(
        accepted_types=[".csv"],
        min_count=1,
        max_count=3,
        max_file_size=1_000_000,
    )
    defaults.update(overrides)
    return FileRequirements(**defaults)


def _make_convention(**overrides) -> Convention:
    defaults = dict(
        srn=_make_conv_srn(),
        title="Test Convention",
        schema_srn=_make_schema_srn(),
        file_requirements=_make_file_reqs(),
        created_at=datetime.now(UTC),
    )
    defaults.update(overrides)
    return Convention(**defaults)


def _make_deposition(**overrides) -> Deposition:
    defaults = dict(
        srn=_make_dep_srn(),
        convention_srn=_make_conv_srn(),
        owner_id=UserId(uuid4()),
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    defaults.update(overrides)
    return Deposition(**defaults)


def _make_service(
    dep_repo=None,
    conv_repo=None,
    file_storage=None,
    outbox=None,
) -> DepositionService:
    return DepositionService(
        deposition_repo=dep_repo or AsyncMock(),
        convention_repo=conv_repo or AsyncMock(),
        file_storage=file_storage or AsyncMock(),
        outbox=outbox or AsyncMock(),
        node_domain=Domain("localhost"),
    )


class TestDepositionServiceCreate:
    @pytest.mark.asyncio
    async def test_create_with_valid_convention(self):
        conv_repo = AsyncMock()
        conv_repo.get.return_value = _make_convention()
        owner = UserId(uuid4())

        service = _make_service(conv_repo=conv_repo)
        result = await service.create(
            convention_srn=_make_conv_srn(),
            owner_id=owner,
        )
        assert result.convention_srn == _make_conv_srn()
        assert result.owner_id == owner
        assert result.status == DepositionStatus.DRAFT

    @pytest.mark.asyncio
    async def test_create_rejects_nonexistent_convention(self):
        conv_repo = AsyncMock()
        conv_repo.get.return_value = None

        service = _make_service(conv_repo=conv_repo)
        with pytest.raises(NotFoundError, match="Convention not found"):
            await service.create(
                convention_srn=_make_conv_srn(),
                owner_id=UserId(uuid4()),
            )

    @pytest.mark.asyncio
    async def test_create_generates_srn(self):
        conv_repo = AsyncMock()
        conv_repo.get.return_value = _make_convention()

        service = _make_service(conv_repo=conv_repo)
        result = await service.create(
            convention_srn=_make_conv_srn(),
            owner_id=UserId(uuid4()),
        )
        assert str(result.srn).startswith("urn:osa:localhost:dep:")

    @pytest.mark.asyncio
    async def test_create_saves_to_repo(self):
        conv_repo = AsyncMock()
        conv_repo.get.return_value = _make_convention()
        dep_repo = AsyncMock()

        service = _make_service(dep_repo=dep_repo, conv_repo=conv_repo)
        await service.create(
            convention_srn=_make_conv_srn(),
            owner_id=UserId(uuid4()),
        )
        dep_repo.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_emits_deposition_created_event(self):
        conv_srn = _make_conv_srn()
        conv_repo = AsyncMock()
        conv_repo.get.return_value = _make_convention(srn=conv_srn)
        outbox = AsyncMock()
        owner = UserId(uuid4())

        service = _make_service(conv_repo=conv_repo, outbox=outbox)
        result = await service.create(convention_srn=conv_srn, owner_id=owner)

        outbox.append.assert_called_once()
        event = outbox.append.call_args[0][0]
        assert isinstance(event, DepositionCreatedEvent)
        assert event.deposition_id == result.srn
        assert event.convention_srn == conv_srn
        assert event.owner_id == owner


class TestDepositionServiceUpdateMetadata:
    @pytest.mark.asyncio
    async def test_update_metadata(self):
        dep = _make_deposition()
        dep_repo = AsyncMock()
        dep_repo.get.return_value = dep

        service = _make_service(dep_repo=dep_repo)
        result = await service.update_metadata(dep.srn, {"title": "Updated"})
        assert result.metadata == {"title": "Updated"}
        dep_repo.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_metadata_not_found(self):
        dep_repo = AsyncMock()
        dep_repo.get.return_value = None

        service = _make_service(dep_repo=dep_repo)
        with pytest.raises(NotFoundError):
            await service.update_metadata(_make_dep_srn(), {"title": "Test"})

    @pytest.mark.asyncio
    async def test_update_metadata_emits_event(self):
        dep = _make_deposition()
        dep_repo = AsyncMock()
        dep_repo.get.return_value = dep
        outbox = AsyncMock()

        service = _make_service(dep_repo=dep_repo, outbox=outbox)
        metadata = {"title": "Updated"}
        await service.update_metadata(dep.srn, metadata)

        outbox.append.assert_called_once()
        event = outbox.append.call_args[0][0]
        assert isinstance(event, MetadataUpdatedEvent)
        assert event.deposition_id == dep.srn
        assert event.metadata == metadata


class TestDepositionServiceUploadFile:
    @pytest.mark.asyncio
    async def test_upload_file_success(self):
        dep = _make_deposition()
        conv = _make_convention(file_requirements=_make_file_reqs(accepted_types=[".csv"]))
        dep_repo = AsyncMock()
        dep_repo.get.return_value = dep
        conv_repo = AsyncMock()
        conv_repo.get.return_value = conv
        file_storage = AsyncMock()
        saved_file = DepositionFile(
            name="data.csv", size=500, checksum="abc", uploaded_at=datetime.now(UTC)
        )
        file_storage.save_file.return_value = saved_file

        service = _make_service(dep_repo=dep_repo, conv_repo=conv_repo, file_storage=file_storage)
        result = await service.upload_file(dep.srn, "data.csv", b"content", 500)
        assert len(result.files) == 1
        file_storage.save_file.assert_called_once()

    @pytest.mark.asyncio
    async def test_upload_file_rejects_wrong_type(self):
        dep = _make_deposition()
        conv = _make_convention(file_requirements=_make_file_reqs(accepted_types=[".csv"]))
        dep_repo = AsyncMock()
        dep_repo.get.return_value = dep
        conv_repo = AsyncMock()
        conv_repo.get.return_value = conv

        service = _make_service(dep_repo=dep_repo, conv_repo=conv_repo)
        with pytest.raises(ValidationError, match="File type"):
            await service.upload_file(dep.srn, "data.xlsx", b"content", 500)

    @pytest.mark.asyncio
    async def test_upload_file_rejects_exceeds_max_size(self):
        dep = _make_deposition()
        conv = _make_convention(file_requirements=_make_file_reqs(max_file_size=100))
        dep_repo = AsyncMock()
        dep_repo.get.return_value = dep
        conv_repo = AsyncMock()
        conv_repo.get.return_value = conv

        service = _make_service(dep_repo=dep_repo, conv_repo=conv_repo)
        with pytest.raises(ValidationError, match="exceeds maximum"):
            await service.upload_file(dep.srn, "data.csv", b"x" * 200, 200)

    @pytest.mark.asyncio
    async def test_upload_file_rejects_exceeds_max_count(self):
        dep = _make_deposition(
            files=[
                DepositionFile(
                    name=f"f{i}.csv", size=10, checksum="x", uploaded_at=datetime.now(UTC)
                )
                for i in range(3)
            ]
        )
        conv = _make_convention(file_requirements=_make_file_reqs(max_count=3))
        dep_repo = AsyncMock()
        dep_repo.get.return_value = dep
        conv_repo = AsyncMock()
        conv_repo.get.return_value = conv

        service = _make_service(dep_repo=dep_repo, conv_repo=conv_repo)
        with pytest.raises(ValidationError, match="Maximum.*files"):
            await service.upload_file(dep.srn, "extra.csv", b"content", 500)

    @pytest.mark.asyncio
    async def test_upload_file_emits_event(self):
        dep = _make_deposition()
        conv = _make_convention(file_requirements=_make_file_reqs(accepted_types=[".csv"]))
        dep_repo = AsyncMock()
        dep_repo.get.return_value = dep
        conv_repo = AsyncMock()
        conv_repo.get.return_value = conv
        file_storage = AsyncMock()
        saved_file = DepositionFile(
            name="data.csv", size=500, checksum="abc123", uploaded_at=datetime.now(UTC)
        )
        file_storage.save_file.return_value = saved_file
        outbox = AsyncMock()

        service = _make_service(
            dep_repo=dep_repo, conv_repo=conv_repo, file_storage=file_storage, outbox=outbox
        )
        await service.upload_file(dep.srn, "data.csv", b"content", 500)

        outbox.append.assert_called_once()
        event = outbox.append.call_args[0][0]
        assert isinstance(event, FileUploadedEvent)
        assert event.deposition_id == dep.srn
        assert event.filename == "data.csv"
        assert event.size == 500
        assert event.checksum == "abc123"


class TestDepositionServiceDeleteFile:
    @pytest.mark.asyncio
    async def test_delete_file_success(self):
        dep = _make_deposition(
            files=[
                DepositionFile(name="a.csv", size=10, checksum="x", uploaded_at=datetime.now(UTC))
            ]
        )
        dep_repo = AsyncMock()
        dep_repo.get.return_value = dep
        file_storage = AsyncMock()

        service = _make_service(dep_repo=dep_repo, file_storage=file_storage)
        result = await service.delete_file(dep.srn, "a.csv")
        assert len(result.files) == 0
        file_storage.delete_file.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_file_not_found(self):
        dep_repo = AsyncMock()
        dep_repo.get.return_value = None

        service = _make_service(dep_repo=dep_repo)
        with pytest.raises(NotFoundError):
            await service.delete_file(_make_dep_srn(), "a.csv")

    @pytest.mark.asyncio
    async def test_delete_file_emits_event(self):
        dep = _make_deposition(
            files=[
                DepositionFile(name="a.csv", size=10, checksum="x", uploaded_at=datetime.now(UTC))
            ]
        )
        dep_repo = AsyncMock()
        dep_repo.get.return_value = dep
        file_storage = AsyncMock()
        outbox = AsyncMock()

        service = _make_service(dep_repo=dep_repo, file_storage=file_storage, outbox=outbox)
        await service.delete_file(dep.srn, "a.csv")

        outbox.append.assert_called_once()
        event = outbox.append.call_args[0][0]
        assert isinstance(event, FileDeletedEvent)
        assert event.deposition_id == dep.srn
        assert event.filename == "a.csv"


class TestDepositionServiceSubmit:
    @pytest.mark.asyncio
    async def test_submit_with_enough_files(self):
        dep = _make_deposition(
            files=[
                DepositionFile(name="a.csv", size=10, checksum="x", uploaded_at=datetime.now(UTC))
            ]
        )
        conv = _make_convention(file_requirements=_make_file_reqs(min_count=1))
        dep_repo = AsyncMock()
        dep_repo.get.return_value = dep
        conv_repo = AsyncMock()
        conv_repo.get.return_value = conv
        outbox = AsyncMock()

        service = _make_service(dep_repo=dep_repo, conv_repo=conv_repo, outbox=outbox)
        result = await service.submit(dep.srn)
        assert result.status == DepositionStatus.IN_VALIDATION
        outbox.append.assert_called_once()
        event = outbox.append.call_args[0][0]
        assert isinstance(event, DepositionSubmittedEvent)

    @pytest.mark.asyncio
    async def test_submit_rejects_too_few_files(self):
        dep = _make_deposition(files=[])
        conv = _make_convention(file_requirements=_make_file_reqs(min_count=1))
        dep_repo = AsyncMock()
        dep_repo.get.return_value = dep
        conv_repo = AsyncMock()
        conv_repo.get.return_value = conv

        service = _make_service(dep_repo=dep_repo, conv_repo=conv_repo)
        with pytest.raises(ValidationError, match="Minimum.*file"):
            await service.submit(dep.srn)

    @pytest.mark.asyncio
    async def test_submit_not_found(self):
        dep_repo = AsyncMock()
        dep_repo.get.return_value = None

        service = _make_service(dep_repo=dep_repo)
        with pytest.raises(NotFoundError):
            await service.submit(_make_dep_srn())


class TestDepositionServiceListDepositions:
    @pytest.mark.asyncio
    async def test_list_by_owner_calls_repo(self):
        owner = UserId(uuid4())
        dep_repo = AsyncMock()
        dep_repo.list_by_owner.return_value = []
        dep_repo.count_by_owner.return_value = 0

        service = _make_service(dep_repo=dep_repo)
        items, total = await service.list_depositions(owner)

        dep_repo.list_by_owner.assert_called_once_with(owner, limit=None, offset=None)
        dep_repo.count_by_owner.assert_called_once_with(owner)
        assert items == []
        assert total == 0

    @pytest.mark.asyncio
    async def test_list_all_when_owner_is_none(self):
        dep_repo = AsyncMock()
        dep_repo.list.return_value = [_make_deposition(), _make_deposition()]
        dep_repo.count.return_value = 2

        service = _make_service(dep_repo=dep_repo)
        items, total = await service.list_depositions(None)

        dep_repo.list.assert_called_once_with(limit=None, offset=None)
        dep_repo.count.assert_called_once()
        assert len(items) == 2
        assert total == 2

    @pytest.mark.asyncio
    async def test_list_depositions_returns_total(self):
        owner = UserId(uuid4())
        deps = [_make_deposition(owner_id=owner), _make_deposition(owner_id=owner)]
        dep_repo = AsyncMock()
        dep_repo.list_by_owner.return_value = deps
        dep_repo.count_by_owner.return_value = 5  # more than page

        service = _make_service(dep_repo=dep_repo)
        items, total = await service.list_depositions(owner)

        assert len(items) == 2
        assert total == 5

    @pytest.mark.asyncio
    async def test_list_depositions_passes_limit_offset(self):
        owner = UserId(uuid4())
        dep_repo = AsyncMock()
        dep_repo.list_by_owner.return_value = []
        dep_repo.count_by_owner.return_value = 0

        service = _make_service(dep_repo=dep_repo)
        await service.list_depositions(owner, limit=10, offset=20)

        dep_repo.list_by_owner.assert_called_once_with(owner, limit=10, offset=20)


class TestDepositionServiceGetFileDownload:
    @pytest.mark.asyncio
    async def test_raises_if_file_not_in_deposition(self):
        dep = _make_deposition(files=[])
        dep_repo = AsyncMock()
        dep_repo.get.return_value = dep

        service = _make_service(dep_repo=dep_repo)
        with pytest.raises(NotFoundError, match="File.*not found"):
            await service.get_file_download(dep.srn, "missing.csv")

    @pytest.mark.asyncio
    async def test_returns_stream_and_metadata(self):
        file_meta = DepositionFile(
            name="data.csv", size=100, checksum="abc", uploaded_at=datetime.now(UTC)
        )
        dep = _make_deposition(files=[file_meta])
        dep_repo = AsyncMock()
        dep_repo.get.return_value = dep
        file_storage = AsyncMock()

        async def _fake_stream():
            yield b"chunk"

        file_storage.get_file.return_value = _fake_stream()

        service = _make_service(dep_repo=dep_repo, file_storage=file_storage)
        stream, returned_meta = await service.get_file_download(dep.srn, "data.csv")

        file_storage.get_file.assert_called_once_with(dep.srn, "data.csv")
        assert stream is not None
        assert returned_meta.name == "data.csv"
        assert returned_meta.size == 100
