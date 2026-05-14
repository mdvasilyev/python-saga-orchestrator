from enum import StrEnum


class SagaStatus(StrEnum):
    RUNNING = "RUNNING"
    SUSPENDED = "SUSPENDED"
    FAILED = "FAILED"
    COMPENSATING = "COMPENSATING"
    COMPLETED = "COMPLETED"
    COMPENSATING_SUSPENDED = "COMPENSATING_SUSPENDED"

    @property
    def is_terminal(self) -> bool:
        return self in {self.FAILED, self.COMPLETED}
