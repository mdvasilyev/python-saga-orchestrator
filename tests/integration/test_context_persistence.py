from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from saga_orchestrator.domain.models.context import SagaContext
from saga_orchestrator.domain.models.enums import SagaStepPhase, SagaStepStatus
from tests.integration.helpers import StartInput
from tests.integration.models import IntegrationSagaState


async def _create_saga(
    session: AsyncSession, aggregation_id: str
) -> tuple[uuid.UUID, SagaContext]:
    """Helper to create and persist a saga with a basic context."""
    saga_id = uuid.uuid4()
    context = SagaContext(
        saga_id=saga_id,
        saga_name="test_saga",
        initial_data={"foo": "bar"},
    )
    saga = IntegrationSagaState(
        id=saga_id,
        aggregation_id=aggregation_id,
        saga_name=f"saga_{saga_id}",
        trace_id=str(uuid.uuid4()),
        context=context,
    )
    session.add(saga)
    await session.commit()
    return saga_id, context


@pytest.mark.asyncio
async def test_create_and_read_context(session_maker):
    """
    Проверяет базовый сценарий: создание саги с контекстом, сохранение в БД,
    и последующее чтение с проверкой типов и данных.
    """
    agg_id = "test-create-read"
    original_saga_id, original_context = await _create_saga(session_maker(), agg_id)

    async with session_maker() as session:
        loaded_saga = await session.get(IntegrationSagaState, original_saga_id)

        assert loaded_saga is not None
        assert isinstance(loaded_saga.context, SagaContext)
        assert loaded_saga.context.saga_id == original_context.saga_id
        assert loaded_saga.context.saga_name == original_context.saga_name
        assert loaded_saga.context.initial_data == original_context.initial_data

        # Коллекции теперь иммутабельны (MappingProxyType и tuple)
        assert loaded_saga.context.step_outputs == {}
        assert loaded_saga.context.processed_event_ids == ()


@pytest.mark.asyncio
async def test_top_level_attribute_change_is_persisted(session_maker):
    """
    Проверяет, что изменение атрибута верхнего уровня в контексте отслеживается
    и сохраняется в БД благодаря __setattr__ в MutableModel.
    """
    agg_id = "test-top-level"
    saga_id, _ = await _create_saga(session_maker(), agg_id)
    new_awaiting_type = "some.new.event"

    async with session_maker() as session:
        saga = await session.get(IntegrationSagaState, saga_id)
        saga.context.awaiting_event_type = new_awaiting_type
        await session.commit()

    async with session_maker() as session:
        reloaded_saga = await session.get(IntegrationSagaState, saga_id)
        assert reloaded_saga.context.awaiting_event_type == new_awaiting_type


@pytest.mark.asyncio
async def test_nested_list_mutation_is_forbidden(session_maker):
    """
    Проверяет, что архитектура защищает от прямых мутаций списка
    """
    agg_id = "test-list-forbidden"
    saga_id, _ = await _create_saga(session_maker(), agg_id)

    async with session_maker() as session:
        saga = await session.get(IntegrationSagaState, saga_id)

        with pytest.raises(
            AttributeError, match="'tuple' object has no attribute 'append'"
        ):
            saga.context.processed_event_ids.append("evt-12345")


@pytest.mark.asyncio
async def test_nested_list_mutation_is_persisted_via_mutator(session_maker):
    """
    Проверяет правильный способ сохранения списков: через метод-мутатор (переприсваивание).
    """
    agg_id = "test-list-saved"
    saga_id, _ = await _create_saga(session_maker(), agg_id)
    event_id_to_add = "evt-abcde"

    async with session_maker() as session:
        saga = await session.get(IntegrationSagaState, saga_id)
        saga.context.add_processed_event(event_id_to_add)
        await session.commit()

    async with session_maker() as session:
        reloaded_saga = await session.get(IntegrationSagaState, saga_id)
        assert reloaded_saga.context.processed_event_ids == (event_id_to_add,)


@pytest.mark.asyncio
async def test_nested_dict_mutation_is_forbidden(session_maker):
    """
    Проверяет, что словарь закрыт за MappingProxyType, и прямое изменение вызовет ошибку.
    """
    agg_id = "test-dict-forbidden"
    saga_id, _ = await _create_saga(session_maker(), agg_id)

    async with session_maker() as session:
        saga = await session.get(IntegrationSagaState, saga_id)

        with pytest.raises(TypeError, match="does not support item assignment"):
            saga.context.step_outputs["step_1"] = {"result": "success"}


@pytest.mark.asyncio
async def test_nested_dict_mutation_is_persisted_via_mutator(session_maker):
    """
    Показывает правильный способ сохранения изменений во вложенных словарях
    без ручного вызова flag_modified().
    """
    agg_id = "test-dict-saved"
    saga_id, _ = await _create_saga(session_maker(), agg_id)
    output_data = {"result": "success"}

    async with session_maker() as session:
        saga = await session.get(IntegrationSagaState, saga_id)
        saga.context.save_step_output("step_1", output_data)
        await session.commit()

    async with session_maker() as session:
        reloaded_saga = await session.get(IntegrationSagaState, saga_id)
        assert reloaded_saga.context.step_outputs["step_1"] == output_data


@pytest.mark.asyncio
async def test_context_is_fully_replaced(session_maker):
    """
    Проверяет, что полная замена объекта context работает корректно.
    """
    agg_id = "test-replace"
    saga_id, _ = await _create_saga(session_maker(), agg_id)

    new_context = SagaContext(
        saga_id=saga_id, saga_name="new_test_saga", initial_data={"baz": "qux"}
    )

    async with session_maker() as session:
        saga = await session.get(IntegrationSagaState, saga_id)
        saga.context = new_context
        await session.commit()

    async with session_maker() as session:
        reloaded_saga = await session.get(IntegrationSagaState, saga_id)
        assert reloaded_saga.context.saga_name == "new_test_saga"
        assert reloaded_saga.context.initial_data == {"baz": "qux"}
        assert reloaded_saga.context.saga_id == saga_id


@pytest.mark.asyncio
async def test_complex_object_in_any_field_is_serialized(session_maker):
    """
    Проверяет, что сложный объект (Pydantic-модель) в поле `Any`
    корректно сериализуется в словарь при сохранении.
    """
    agg_id = "test-any-type"
    saga_id = uuid.uuid4()
    saga_name = "test_saga"
    initial_obj = StartInput(value=123)
    context = SagaContext(
        saga_id=saga_id,
        saga_name=saga_name,
        initial_data=initial_obj,
    )
    saga = IntegrationSagaState(
        id=saga_id,
        aggregation_id=agg_id,
        saga_name=saga_name,
        trace_id=str(uuid.uuid4()),
        context=context,
    )

    async with session_maker() as session:
        session.add(saga)
        await session.commit()

    async with session_maker() as session:
        loaded_saga = await session.get(IntegrationSagaState, saga_id)
        assert loaded_saga.context.initial_data == {"value": 123}
        reconstructed_obj = StartInput.model_validate(loaded_saga.context.initial_data)
        assert reconstructed_obj == initial_obj


@pytest.mark.asyncio
async def test_proves_data(session_maker):
    agg_id = "test-data"
    saga_id, _ = await _create_saga(session_maker(), agg_id)
    event_id_to_add = "evt"

    async with session_maker() as session:
        saga = await session.get(IntegrationSagaState, saga_id)
        assert saga.context.processed_event_ids == ()

        saga.context.add_processed_event(event_id_to_add)
        await session.commit()

    async with session_maker() as session:
        reloaded_saga = await session.get(IntegrationSagaState, saga_id)
        assert reloaded_saga.context.processed_event_ids == (event_id_to_add,)


@pytest.mark.asyncio
async def test_step_history_append_is_persisted(session_maker):
    agg_id = "test-history-append"
    saga_id, _ = await _create_saga(session_maker(), agg_id)

    history_entry = {
        "timestamp": datetime.now(UTC).isoformat(),
        "phase": SagaStepPhase.EXECUTE,
        "status": SagaStepStatus.ERROR,
        "step_id": "test_step",
        "step_name": "MyTestStep",
        "attempt": 1,
        "token": str(uuid.uuid4()),
        "input": {"some": "data"},
        "output": None,
        "error": "Some error occurred",
    }

    async with session_maker() as session:
        saga = await session.get(IntegrationSagaState, saga_id)

        assert len(saga.step_history) == 0
        saga.step_history.append(history_entry)
        await session.commit()

    async with session_maker() as session:
        reloaded_saga = await session.get(IntegrationSagaState, saga_id)
        assert len(reloaded_saga.step_history) == 1
        saved_entry = reloaded_saga.step_history[0]

        assert saved_entry["step_id"] == "test_step"
        assert saved_entry["status"] == SagaStepStatus.ERROR
        assert saved_entry["error"] == "Some error occurred"
