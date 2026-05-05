from sqlalchemy.orm import DeclarativeBase

from saga_orchestrator.domain.mixins import SagaStateMixin
from saga_orchestrator.inbox.models import InboxMessageMixin
from saga_orchestrator.outbox.models import OutboxMessageMixin


class Base(DeclarativeBase):
    pass


class IntegrationSagaState(Base, SagaStateMixin):
    __tablename__ = "test_saga_state"


class IntegrationOutboxMessage(Base, OutboxMessageMixin):
    __tablename__ = "test_outbox_messages"


class IntegrationInboxMessage(Base, InboxMessageMixin):
    __tablename__ = "test_inbox_messages"
