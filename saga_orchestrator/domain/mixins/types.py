from typing import TypeVar

from pydantic import BaseModel
from sqlalchemy import JSON, TypeDecorator
from sqlalchemy.ext.mutable import Mutable

Model = TypeVar("Model", bound=BaseModel)


class JsonPydanticField(TypeDecorator):
    impl = JSON

    def __init__(self, pydantic_model):
        super().__init__()
        self.pydantic_model = pydantic_model

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(JSON())

    def process_bind_param(self, value: BaseModel, _):
        return value.model_dump(mode="json") if value is not None else None

    def process_result_value(self, value, _):
        return self.pydantic_model.model_validate(value) if value is not None else None


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
