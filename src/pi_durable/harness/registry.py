"""Application-owned extension registry and immutable published snapshots."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from .agent import INSTRUCTIONS_KEY
from .generation import GenerationTask
from .tool import ToolTask
from .types import (
    AnyTask,
    Extension,
    Registry,
    RegistrySnapshot,
)

_SECTION_KEY = re.compile(r"^[a-z][a-z0-9_-]*$")

#: Built-in task definitions every registry holds. The compaction task joins with its ticket.
BUILTIN_TASKS: tuple[AnyTask, ...] = (GenerationTask, ToolTask)


class _RegistryState:
    """Immutable published registry state."""

    __slots__ = ("_by_name", "_extensions", "_tasks")

    def __init__(self, extensions: tuple[Extension, ...]) -> None:
        self._extensions = extensions
        self._by_name = {extension.name: extension for extension in extensions}
        tasks: dict[str, AnyTask] = {}
        for task in BUILTIN_TASKS:
            tasks[task.definition.name] = task
        for extension in extensions:
            for task in extension.tasks:
                name = task.definition.name
                if name in tasks:
                    raise ValueError(f"Task {name} of extension {extension.name} is already installed")
                tasks[name] = task
        self._tasks = tasks

    def installed(self) -> tuple[Extension, ...]:
        return self._extensions

    def extension(self, name: str) -> Extension | None:
        return self._by_name.get(name)

    def tools(self) -> list[dict[str, Any]]:
        return [
            {"extension": extension, "tool": tool}
            for extension in self._extensions
            for tool in extension.tools
        ]

    def sections(self) -> list[dict[str, Any]]:
        return [
            {"extension": extension, "section": section}
            for extension in self._extensions
            for section in extension.sections
        ]

    def tasks(self) -> list[AnyTask]:
        return list(self._tasks.values())

    def task(self, name: str) -> AnyTask | None:
        return self._tasks.get(name)


class _Registry:
    """Application-owned registry of extensions."""

    def __init__(self) -> None:
        self._current = _RegistryState(())
        self._listeners: set[Callable[[], Any]] = set()

    def snapshot(self) -> RegistrySnapshot:
        return self._current

    def subscribe(self, listener: Callable[[], Any]) -> Callable[[], None]:
        self._listeners.add(listener)
        return lambda: self._listeners.discard(listener)

    def install(self, extension: Extension) -> None:
        _validate_extension(extension)
        current = self._current.installed()
        index = next((i for i, installed in enumerate(current) if installed.name == extension.name), -1)
        if index < 0:
            following = (*current, extension)
        else:
            following = (*current[:index], extension, *current[index + 1 :])
        self._publish(following)

    def uninstall(self, extension: Extension) -> None:
        current = self._current.installed()
        if not any(installed.name == extension.name for installed in current):
            return
        self._publish(tuple(installed for installed in current if installed.name != extension.name))

    def _publish(self, extensions: tuple[Extension, ...]) -> None:
        # Build and validate the next state first, so a collision publishes nothing.
        self._current = _RegistryState(extensions)
        for listener in list(self._listeners):
            listener()


def _validate_extension(extension: Extension) -> None:
    tools: set[str] = set()
    for tool in extension.tools:
        if tool.name in tools:
            raise ValueError(f"Extension {extension.name} has two tools named {tool.name}")
        tools.add(tool.name)
    sections: set[str] = set()
    for section in extension.sections:
        if not _SECTION_KEY.match(section.key):
            raise TypeError(f"Section key {section.key!r} must match {_SECTION_KEY.pattern}")
        if section.key == INSTRUCTIONS_KEY:
            raise ValueError(f"Section key {section.key} is reserved for the agent's instructions")
        if section.key in sections:
            raise ValueError(f"Extension {extension.name} has two sections with key {section.key}")
        sections.add(section.key)


def create_registry() -> Registry:
    """Create an application-owned registry holding only the built-in tasks."""

    return _Registry()


__all__ = [
    "BUILTIN_TASKS",
    "INSTRUCTIONS_KEY",
    "create_registry",
]
