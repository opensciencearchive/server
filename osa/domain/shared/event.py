from abc import ABC
from pydantic import BaseModel

from osa.domain.shared.model.srn import EventSRN


class Event(BaseModel, ABC):
    srn: EventSRN


class EventBus(ABC): ...
