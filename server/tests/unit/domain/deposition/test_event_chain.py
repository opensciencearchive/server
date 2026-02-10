"""Unit tests verifying the event chain: DepositionSubmitted → Validate → Approve → Record."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from unittest.mock import MagicMock
from osa.domain.curation.handler.auto_approve_curation import AutoApproveCuration
from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.record.handler.convert_deposition_to_record import ConvertDepositionToRecord
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import DepositionSRN, ValidationRunSRN
from osa.domain.validation.event.validation_completed import ValidationCompleted
from osa.domain.validation.handler.validate_deposition import ValidateDeposition
from osa.domain.validation.model import RunStatus


def _make_dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test-dep")


def _make_config():
    """Create a minimal Config mock for testing."""
    config = MagicMock()
    config.server.domain = "localhost"
    return config


class TestValidateDepositionEmitsCompleted:
    @pytest.mark.asyncio
    async def test_emits_validation_completed(self):
        outbox = AsyncMock()
        config = _make_config()
        handler = ValidateDeposition(outbox=outbox, config=config)

        event = DepositionSubmittedEvent(
            id=EventId(uuid4()),
            deposition_id=_make_dep_srn(),
            metadata={"title": "Test"},
        )
        await handler.handle(event)

        outbox.append.assert_called_once()
        emitted = outbox.append.call_args[0][0]
        assert isinstance(emitted, ValidationCompleted)
        assert emitted.deposition_srn == _make_dep_srn()
        assert emitted.status == RunStatus.COMPLETED


class TestAutoApproveCurationEmitsApproved:
    @pytest.mark.asyncio
    async def test_auto_approve_on_completed(self):
        outbox = AsyncMock()
        handler = AutoApproveCuration(outbox=outbox)

        event = ValidationCompleted(
            id=EventId(uuid4()),
            validation_run_srn=ValidationRunSRN.parse("urn:osa:localhost:val:run1"),
            deposition_srn=_make_dep_srn(),
            status=RunStatus.COMPLETED,
            results=[],
            metadata={"title": "Test"},
        )
        await handler.handle(event)

        outbox.append.assert_called_once()

    @pytest.mark.asyncio
    async def test_skips_approve_on_failed(self):
        outbox = AsyncMock()
        handler = AutoApproveCuration(outbox=outbox)

        event = ValidationCompleted(
            id=EventId(uuid4()),
            validation_run_srn=ValidationRunSRN.parse("urn:osa:localhost:val:run1"),
            deposition_srn=_make_dep_srn(),
            status=RunStatus.FAILED,
            results=[],
            metadata={"title": "Test"},
        )
        await handler.handle(event)

        outbox.append.assert_not_called()


class TestConvertDepositionToRecord:
    @pytest.mark.asyncio
    async def test_publishes_record(self):
        service = AsyncMock()
        handler = ConvertDepositionToRecord(service=service)

        from osa.domain.curation.event.deposition_approved import DepositionApproved

        event = DepositionApproved(
            id=EventId(uuid4()),
            deposition_srn=_make_dep_srn(),
            metadata={"title": "Test"},
        )
        await handler.handle(event)

        service.publish_record.assert_called_once_with(
            deposition_srn=_make_dep_srn(),
            metadata={"title": "Test"},
        )
