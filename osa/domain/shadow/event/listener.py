from datetime import datetime
import logfire

from osa.domain.shadow.model.report import ShadowReport
from osa.domain.shadow.model.value import ShadowStatus
from osa.domain.shadow.port.repository import ShadowRepository
from osa.domain.shared.event import EventListener
from osa.domain.validation.event.validation_completed import ValidationCompleted
from osa.domain.deposition.command.delete_files import (
    DeleteDepositionFiles,
    DeleteDepositionFilesHandler,
)


class ValidationCompletedListener(EventListener[ValidationCompleted]):
    repo: ShadowRepository
    delete_files_handler: DeleteDepositionFilesHandler

    def handle(self, event: ValidationCompleted) -> None:
        with logfire.span("Shadow.ValidationCompletedListener"):
            # 1. Check if relevant
            req = self.repo.get_request_by_deposition_id(event.deposition_id)
            if not req:
                return

            # 2. Update Request Status
            req.status = ShadowStatus.COMPLETED
            self.repo.save_request(req)

            # 3. Create Report
            # Naive score calculation
            score = "N/A"

            report = ShadowReport(
                shadow_id=req.id,
                source_domain=req.source_url,  # simplistic domain extraction
                validation_summary=event.summary,
                score=score,
                created_at=datetime.now(),
            )
            self.repo.save_report(report)

            # 4. Cleanup Files (Ephemeral nature)
            cmd = DeleteDepositionFiles(srn=event.deposition_id)
            self.delete_files_handler.run(cmd)

            logfire.info("Shadow analysis completed", shadow_id=req.id)
