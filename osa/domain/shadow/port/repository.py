from abc import abstractmethod
from typing import Optional, Protocol

from osa.domain.shadow.model.aggregate import ShadowId, ShadowRequest
from osa.domain.shadow.model.report import ShadowReport
from osa.domain.shared.model.srn import DepositionSRN
from osa.domain.shared.port import Port


class ShadowRepository(Port, Protocol):
    @abstractmethod
    def save_request(self, req: ShadowRequest) -> None: ...

    @abstractmethod
    def get_request(self, id: ShadowId) -> ShadowRequest: ...

    @abstractmethod
    def get_request_by_deposition_id(
        self, deposition_id: DepositionSRN
    ) -> Optional[ShadowRequest]: ...

    @abstractmethod
    def save_report(self, report: ShadowReport) -> None: ...

    @abstractmethod
    def get_report(self, id: ShadowId) -> Optional[ShadowReport]: ...

    @abstractmethod
    def list_reports(self, limit: int = 20, offset: int = 0) -> list[ShadowReport]: ...
