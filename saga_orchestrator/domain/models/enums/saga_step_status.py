from enum import auto

from . import BaseStrEnum

class SagaStepStatus(BaseStrEnum):
    SUCCESS = auto()
    ERROR = auto()
    WAITING = auto()
