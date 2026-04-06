from sqlalchemy.orm import DeclarativeBase

from saga_orchestrator.domain.mixins import SagaStateMixin


class Base(DeclarativeBase):
    pass


class IntegrationSagaState(Base, SagaStateMixin):
    __tablename__ = "test_saga_state"
