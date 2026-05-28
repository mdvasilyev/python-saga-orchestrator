from enum import auto

from .base_str_enum import BaseStrEnum


class SagaStepStatus(BaseStrEnum):
    SUCCESS = auto()
    ERROR = auto()
    WAITING = auto()
    TIMED_OUT = auto()
