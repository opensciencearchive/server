from typing import AsyncIterable

import aiodocker
from dishka import provide

from osa.config import Config
from osa.domain.source.port.source_runner import SourceRunner
from osa.domain.validation.port.hook_runner import HookRunner
from osa.infrastructure.oci.runner import OciHookRunner
from osa.infrastructure.oci.source_runner import OciSourceRunner
from osa.util.di.base import Provider
from osa.util.di.scope import Scope


class OciProvider(Provider):
    @provide(scope=Scope.APP)
    async def get_docker(self) -> AsyncIterable[aiodocker.Docker]:
        docker = aiodocker.Docker()
        yield docker
        await docker.close()

    @provide(scope=Scope.UOW)
    def get_hook_runner(self, docker: aiodocker.Docker, config: Config) -> HookRunner:
        return OciHookRunner(docker=docker, host_data_dir=config.host_data_dir)

    @provide(scope=Scope.UOW)
    def get_source_runner(self, docker: aiodocker.Docker, config: Config) -> SourceRunner:
        return OciSourceRunner(docker=docker, host_data_dir=config.host_data_dir)
