from typing import Type, TypeVar

from pydantic import BaseModel
from sqlalchemy import JSON, TypeDecorator
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import Mutable

Model = TypeVar("Model", bound=BaseModel)


class JsonPydanticField(TypeDecorator):
    impl = JSON

    def __init__(self, pydantic_model):
        super().__init__()
        self.pydantic_model = pydantic_model

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB())
        return dialect.type_descriptor(JSON())

    def process_bind_param(self, value: Model | None, dialect):
        return value.model_dump(mode="json") if value is not None else None

    def process_result_value(self, value: list[dict] | None, dialect):
        return self.pydantic_model.model_validate(value) if value is not None else None


class JsonPydanticListField(TypeDecorator):
    impl = JSON

    def __init__(self, pydantic_model: Type[Model]):
        super().__init__()
        self.pydantic_model = pydantic_model

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB())
        return dialect.type_descriptor(JSON())

    def process_bind_param(
        self, value: list[Model] | None, dialect
    ) -> list[dict] | None:
        if value is None:
            return None
        return [item.model_dump(mode="json") for item in value]

    def process_result_value(
        self, value: list[dict] | None, dialect
    ) -> list[Model] | None:
        if value is None:
            return None
        return [self.pydantic_model.model_validate(item) for item in value]


def MutableModel(pydantic_model):
    class MutableModel(Mutable, pydantic_model):
        @classmethod
        def coerce(cls, key, value):
            if not isinstance(value, MutableModel):
                if isinstance(value, pydantic_model):
                    return MutableModel.model_validate(value.model_dump(mode="json"))
                if isinstance(value, dict):
                    return MutableModel.model_validate(value)
                return Mutable.coerce(key, value)
            else:
                return value

        def __setattr__(self, key, value):
            super().__setattr__(key, value)
            self.changed()

    return MutableModel.as_mutable(JsonPydanticField(pydantic_model))


def json_type() -> JSON:
    return JSON().with_variant(JSONB, "postgresql")
