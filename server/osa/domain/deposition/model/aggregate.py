from datetime import UTC, datetime
from typing import Any

from osa.domain.auth.model.value import UserId
from osa.domain.deposition.model.value import DepositionFile, DepositionStatus
from osa.domain.shared.error import InvalidStateError
from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN, RecordSRN


class Deposition(Aggregate):
    srn: DepositionSRN
    convention_srn: ConventionSRN
    status: DepositionStatus = DepositionStatus.DRAFT
    metadata: dict[str, Any] = {}
    files: list[DepositionFile] = []
    record_srn: RecordSRN | None = None
    owner_id: UserId
    created_at: datetime
    updated_at: datetime

    def _require_draft(self) -> None:
        if self.status != DepositionStatus.DRAFT:
            raise InvalidStateError(f"Operation not allowed in {self.status} state")

    def update_metadata(self, metadata: dict[str, Any]) -> None:
        self._require_draft()
        self.metadata = metadata
        self.updated_at = datetime.now(UTC)

    def add_file(self, file: DepositionFile) -> None:
        self._require_draft()
        self.files.append(file)
        self.updated_at = datetime.now(UTC)

    def remove_file(self, filename: str) -> DepositionFile:
        self._require_draft()
        for i, f in enumerate(self.files):
            if f.name == filename:
                removed = self.files.pop(i)
                self.updated_at = datetime.now(UTC)
                return removed
        from osa.domain.shared.error import NotFoundError

        raise NotFoundError(f"File '{filename}' not found in deposition")

    def submit(self) -> None:
        self._require_draft()
        self.status = DepositionStatus.IN_VALIDATION
        self.updated_at = datetime.now(UTC)

    def return_to_draft(self) -> None:
        if self.status != DepositionStatus.IN_VALIDATION:
            raise InvalidStateError(
                f"Can only return to draft from IN_VALIDATION, currently {self.status}"
            )
        self.status = DepositionStatus.DRAFT
        self.updated_at = datetime.now(UTC)

    def remove_all_files(self) -> None:
        self.files = []
        self.updated_at = datetime.now(UTC)
