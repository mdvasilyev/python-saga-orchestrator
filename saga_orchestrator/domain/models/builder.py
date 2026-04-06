from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .step import StepDefinition


@dataclass(frozen=True)
class SagaDefinition:
    steps: tuple[StepDefinition[Any, Any], ...]
    compensate_on_failure: bool = True
