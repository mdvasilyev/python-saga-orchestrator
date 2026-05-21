import uuid

from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from saga_orchestrator.domain.mixins import SagaStateMixin
from saga_orchestrator.domain.mixins.saga_step_histrory import SagaStepHistoryMixin
from saga_orchestrator.inbox.models import InboxMessageMixin
from saga_orchestrator.outbox.models import OutboxMessageMixin


class Base(DeclarativeBase):
    pass


class IntegrationSagaHistory(Base, SagaStepHistoryMixin):
    __tablename__ = "test_saga_history"

    saga_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("test_saga_state.id", ondelete="CASCADE"),
        index=True,
    )


class IntegrationSagaState(Base, SagaStateMixin):
    __tablename__ = "test_saga_state"

    step_history: Mapped[list[IntegrationSagaHistory]] = relationship(
        "IntegrationSagaHistory",
        cascade="all, delete-orphan",
        order_by="IntegrationSagaHistory.id",
    )


class IntegrationOutboxMessage(Base, OutboxMessageMixin):
    __tablename__ = "test_outbox_messages"


class IntegrationInboxMessage(Base, InboxMessageMixin):
    __tablename__ = "test_inbox_messages"
