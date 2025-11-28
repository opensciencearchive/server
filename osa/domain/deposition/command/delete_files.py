import logfire

from osa.domain.deposition.port import DepositionRepository, StoragePort
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.srn import DepositionSRN


class DeleteDepositionFiles(Command):
    srn: DepositionSRN


class DepositionFilesDeleted(Result):
    pass


class DeleteDepositionFilesHandler(
    CommandHandler[DeleteDepositionFiles, DepositionFilesDeleted]
):
    repository: DepositionRepository
    storage: StoragePort

    def run(self, cmd: DeleteDepositionFiles) -> DepositionFilesDeleted:
        with logfire.span("DeleteDepositionFiles"):
            # 1. Load deposition
            dep = self.repository.get(cmd.srn)

            # 2. Clear files from aggregate (domain logic)
            dep.remove_all_files()

            # 3. Delete physically (infrastructure)
            self.storage.delete_files_for_deposition(cmd.srn)

            # 4. Persist changes
            self.repository.save(dep)

            return DepositionFilesDeleted()
