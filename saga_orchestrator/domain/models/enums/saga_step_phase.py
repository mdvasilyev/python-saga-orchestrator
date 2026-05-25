from enum import auto

from . import BaseStrEnum


class SagaStepPhase(BaseStrEnum):
    COMPENSATE = auto()
    SUCCESS = auto()
    EXECUTE = auto()
