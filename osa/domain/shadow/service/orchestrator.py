import uuid

import logfire
from osa.domain.deposition.command.create import (
    CreateDeposition,
    CreateDepositionHandler,
)
from osa.domain.deposition.command.submit import (
    SubmitDeposition,
    SubmitDepositionHandler,
)
from osa.domain.deposition.command.upload import UploadFile, UploadFileHandler
from osa.domain.shadow.model.aggregate import ShadowId, ShadowRequest
from osa.domain.shadow.model.value import ShadowStatus
from osa.domain.shadow.port.ingestion import IngestionPort
from osa.domain.shadow.port.repository import ShadowRepository


from osa.domain.shared.model.srn import DepositionProfileSRN


class ShadowOrchestrator:
    def __init__(
        self,
        ingestion: IngestionPort,
        repo: ShadowRepository,
        # In a real app, these would be a CommandBus
        create_dep_handler: CreateDepositionHandler,
        upload_file_handler: UploadFileHandler,
        submit_dep_handler: SubmitDepositionHandler,
    ):
        self.ingestion = ingestion
        self.repo = repo
        self.create_dep_handler = create_dep_handler
        self.upload_file_handler = upload_file_handler
        self.submit_dep_handler = submit_dep_handler

    def start_workflow(self, url: str, profile_srn: str) -> ShadowId:
        shadow_id = ShadowId(str(uuid.uuid4()))

        # Parse profile SRN
        # In real app, we might validate it exists in registry here or in CommandHandler
        profile_srn_obj = DepositionProfileSRN.parse(profile_srn)

        # 1. Create Request Record
        req = ShadowRequest(
            id=shadow_id,
            status=ShadowStatus.PENDING,
            source_url=url,
            profile_srn=profile_srn_obj,
        )
        self.repo.save_request(req)

        try:
            # 2. Ingest
            req.status = ShadowStatus.INGESTING
            self.repo.save_request(req)

            ingest_result = self.ingestion.ingest(url)

            # 3. Create Deposition (Core)
            create_cmd = CreateDeposition(profile_srn=profile_srn_obj)
            dep_result = self.create_dep_handler.run(create_cmd)

            # Update request with deposition ID
            req.deposition_id = dep_result.srn
            self.repo.save_request(req)

            # 4. Upload File
            upload_cmd = UploadFile(
                srn=dep_result.srn,
                filename=ingest_result.filename,
                stream=ingest_result.stream,
            )
            self.upload_file_handler.run(upload_cmd)

            # 5. Submit
            submit_cmd = SubmitDeposition(srn=dep_result.srn)
            self.submit_dep_handler.run(submit_cmd)

            # 6. Update Status (to Validating)
            # Note: Actual validating status might be set by listener or polling,
            # but here we indicate we've handed off to core validation.
            req.status = ShadowStatus.VALIDATING
            self.repo.save_request(req)

            logfire.info("Shadow workflow started", shadow_id=shadow_id)

        except Exception as e:
            req.status = ShadowStatus.FAILED
            self.repo.save_request(req)
            raise e

        return shadow_id
