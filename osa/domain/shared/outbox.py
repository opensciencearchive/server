from abc import ABC, abstractmethod
from typing import NewType
from uuid import UUID

from pydantic import BaseModel

from osa.domain.shared.event import Event


OutboxMessageId = NewType("OutboxMessageId", UUID)


class OutboxMessage(BaseModel, ABC):
    id: OutboxMessageId


class Outbox(ABC):
    @abstractmethod
    async def add(self, event: Event): ...

    @abstractmethod
    async def fetch_batch(self, limit: int = 100) -> list[OutboxMessage]: ...

    @abstractmethod
    async def mark_delivered(self, msg_id: OutboxMessageId): ...

    @abstractmethod
    async def mark_failed(self, msg_id: OutboxMessageId, reason: str): ...
