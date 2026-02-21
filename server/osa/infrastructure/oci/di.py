from typing import AsyncIterable

import aiodocker
from dishka import provide

from osa.domain.validation.port.hook_runner import HookRunner
from osa.infrastructure.oci.runner import OciHookRunner
from osa.util.di.base import Provider
from osa.util.di.scope import Scope


class OciProvider(Provider):
    @provide(scope=Scope.APP)
    async def get_docker(self) -> AsyncIterable[aiodocker.Docker]:
        docker = aiodocker.Docker()
        yield docker
        await docker.close()

    @provide(scope=Scope.UOW)
    def get_runner(self, docker: aiodocker.Docker) -> HookRunner:
        return OciHookRunner(docker=docker)
