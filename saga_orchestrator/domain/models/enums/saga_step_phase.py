from enum import auto

from .base_str_enum import BaseStrEnum


class SagaStepPhase(BaseStrEnum):
    COMPENSATE = auto()
    SUCCESS = auto()
    EXECUTE = auto()
