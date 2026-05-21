from enum import StrEnum, auto


class SagaStepPhase(StrEnum):
    COMPENSATE = auto()
    SUCCESS = auto()
    EXECUTE = auto()
