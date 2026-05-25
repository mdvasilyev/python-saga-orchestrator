from enum import auto

from . import BaseStrEnum


class SagaStatus(BaseStrEnum):
    RUNNING = auto()
    SUSPENDED = auto()
    FAILED = auto()
    COMPENSATING = auto()
    COMPLETED = auto()
    COMPENSATING_SUSPENDED = auto()
    COMPENSATED = auto()

    @property
    def is_terminal(self) -> bool:
        return self in {self.FAILED, self.COMPLETED, self.COMPENSATED}
