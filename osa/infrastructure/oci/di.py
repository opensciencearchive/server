from typing import AsyncIterable

import aiodocker
from dishka import Scope, provide

from osa.domain.validation.port.runner import ValidatorRunner
from osa.infrastructure.oci.runner import DockerValidatorRunner
from osa.util.di.base import Provider


class OciProvider(Provider):
    @provide(scope=Scope.APP)
    async def get_docker(self) -> AsyncIterable[aiodocker.Docker]:
        docker = aiodocker.Docker()
        yield docker
        await docker.close()

    @provide(scope=Scope.REQUEST)
    def get_runner(self, docker: aiodocker.Docker) -> ValidatorRunner:
        return DockerValidatorRunner(docker=docker)
