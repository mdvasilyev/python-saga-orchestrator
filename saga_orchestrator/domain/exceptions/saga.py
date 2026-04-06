class SagaError(Exception):
    """Base exception for orchestrator errors."""


class SagaDefinitionError(SagaError):
    """Invalid saga definition or registration."""


class TypeValidationError(SagaDefinitionError):
    """Type mismatch in step definitions."""


class SagaNotFoundError(SagaError):
    """Saga instance is not found in persistence."""


class ActiveSagaAlreadyExistsError(SagaError):
    """An active saga already exists for the provided aggregation key."""


class SagaStateError(SagaError):
    """Invalid saga state transition or operation."""
