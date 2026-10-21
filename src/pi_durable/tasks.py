"""Executable durable task definitions.

The Session only needs the definition's initial checkpoint and persisted identity; the Harness
later schedules the phase map. Keeping the token here, in the kernel layer, lets transactions
create tasks without importing the harness.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

InputT = TypeVar("InputT")
S = TypeVar("S")
R = TypeVar("R")
H = TypeVar("H")


@dataclass(frozen=True, slots=True)
class TaskDefinition(Generic[InputT, S, R, H]):
    """Definition fields persisted and used when a task is created or recovered."""

    name: str
    version: int
    initial: Callable[[InputT], S]
    phases: Mapping[str, Any]
    abort: Callable[..., Any]
    migrate: Callable[..., Any] | None = None
    hooks: H | None = None

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)


@dataclass(frozen=True, slots=True)
class Task(Generic[InputT, S, R, H]):
    """Typed executable task token."""

    definition: TaskDefinition[InputT, S, R, H]


def _values(definition: Mapping[str, Any] | None, kwargs: Mapping[str, Any]) -> dict[str, Any]:
    values = dict(definition or {})
    values.update(kwargs)
    return values


def define_task(
    definition: Mapping[str, Any] | None = None,
    /,
    **kwargs: Any,
) -> Task[Any, Any, Any, Any]:
    """Define an executable task; register it later with a Harness registry."""

    values = _values(definition, kwargs)
    name = values.get("name")
    if not isinstance(name, str) or not name:
        raise TypeError("Task name must be a non-empty string")
    version = values.get("version")
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise TypeError(f"Task {name} version must be a positive integer")
    initial = values.get("initial")
    if not callable(initial):
        raise TypeError(f"Task {name} requires an initial() function")
    phases = values.get("phases")
    if not isinstance(phases, Mapping):
        raise TypeError(f"Task {name} requires a phases mapping")
    abort = values.get("abort")
    if not callable(abort):
        raise TypeError(f"Task {name} requires an abort() function")
    migrate = values.get("migrate")
    if migrate is not None and not callable(migrate):
        raise TypeError(f"Task {name} migrate must be callable")
    return Task(
        TaskDefinition(
            name=name,
            version=version,
            initial=initial,
            phases=phases,
            abort=abort,
            migrate=migrate,
            hooks=values.get("hooks"),
        )
    )



__all__ = ["Task", "TaskDefinition", "define_task"]
