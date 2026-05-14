from enum import StrEnum, auto


class SagaStepStatus(StrEnum):
    SUCCESS = auto()
    ERROR = auto()
    WAITING = auto()
