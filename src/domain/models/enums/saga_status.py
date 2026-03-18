from enum import Enum


class SagaStatus(str, Enum):
    RUNNING = "RUNNING"
    SUSPENDED = "SUSPENDED"
    FAILED = "FAILED"
    COMPENSATING = "COMPENSATING"
    COMPLETED = "COMPLETED"

    @property
    def is_terminal(self) -> bool:
        return self in {self.FAILED, self.COMPLETED}
