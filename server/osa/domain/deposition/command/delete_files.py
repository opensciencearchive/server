import logfire

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.deposition.port import DepositionRepository, StoragePort
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.srn import DepositionSRN


class DeleteDepositionFiles(Command):
    srn: DepositionSRN


class DepositionFilesDeleted(Result):
    pass


class DeleteDepositionFilesHandler(CommandHandler[DeleteDepositionFiles, DepositionFilesDeleted]):
    __auth__ = at_least(Role.DEPOSITOR)
    principal: Principal
    repository: DepositionRepository
    storage: StoragePort

    async def run(self, cmd: DeleteDepositionFiles) -> DepositionFilesDeleted:
        with logfire.span("DeleteDepositionFiles"):
            # 1. Load deposition
            dep = await self.repository.get(cmd.srn)
            if dep is None:
                raise ValueError(f"Deposition not found: {cmd.srn}")

            # 2. Clear files from aggregate (domain logic)
            dep.remove_all_files()

            # 3. Delete physically (infrastructure)
            self.storage.delete_files_for_deposition(cmd.srn)

            # 4. Persist changes
            await self.repository.save(dep)

            return DepositionFilesDeleted()
