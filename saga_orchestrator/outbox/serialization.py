from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol


class OutboxSerializer(Protocol):
    """Define serialization and deserialization for outbox payload and headers."""

    def serialize_payload(self, payload: Any) -> dict[str, Any]: ...

    def serialize_headers(self, headers: Any) -> dict[str, Any]: ...

    def deserialize_payload(self, payload: Any) -> dict[str, Any]: ...

    def deserialize_headers(self, headers: Any) -> dict[str, Any]: ...


class JsonOutboxSerializer:
    """Serialize payload and headers into JSON-compatible dictionaries."""

    def __init__(self, *, normalize: Callable[[Any], Any] | None = None) -> None:
        self._normalize = normalize

    def serialize_payload(self, payload: Any) -> dict[str, Any]:
        return self._to_dict(payload, field_name="payload")

    def serialize_headers(self, headers: Any) -> dict[str, Any]:
        if headers is None:
            return {}
        return self._to_dict(headers, field_name="headers")

    def deserialize_payload(self, payload: Any) -> dict[str, Any]:
        return self._to_dict(payload, field_name="payload")

    def deserialize_headers(self, headers: Any) -> dict[str, Any]:
        if headers is None:
            return {}
        return self._to_dict(headers, field_name="headers")

    def _to_dict(self, value: Any, *, field_name: str) -> dict[str, Any]:
        normalized = self._normalize(value) if self._normalize is not None else value
        if isinstance(normalized, dict):
            return normalized
        raise TypeError(
            f"Outbox {field_name} must serialize to dict, got {type(value)!r}"
        )
