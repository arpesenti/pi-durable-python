"""Installed extensions, tools, sections, and task definitions.

The registry is process state, never stored: conversations store extension and tool
names, and resolution skips names this process cannot execute.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ..tasks import Task


@dataclass(frozen=True)
class Section:
    name: str
    render: Callable[[Mapping[str, Any]], str | None]
    tag: bool = True


@dataclass(frozen=True)
class Tool:
    """A tool the model or another tool may call."""

    name: str
    description: str
    parameters: dict[str, Any]
    execute: Callable[[dict[str, Any], Any], Any]
    callers: tuple[str, ...] = ("model", "tools")
    replay: str = "unsafe"
    deferred: bool = False
    structured_output_schema: dict[str, Any] | None = None

    @property
    def declaration(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.parameters,
        }


@dataclass(frozen=True)
class Extension:
    name: str
    tools: tuple[Tool, ...] = ()
    sections: tuple[Section, ...] = ()
    tasks: tuple[Task, ...] = ()
    hooks: tuple[Any, ...] = ()


def define_tool(
    *,
    name: str,
    description: str,
    parameters: dict[str, Any] | None = None,
    execute: Callable[[dict[str, Any], Any], Any],
    callers: Sequence[str] = ("model", "tools"),
    replay: str = "unsafe",
    deferred: bool = False,
    structured_output_schema: dict[str, Any] | None = None,
) -> Tool:
    if not name:
        raise TypeError("Tool name must be a non-empty string")
    return Tool(
        name=name,
        description=description,
        parameters=parameters or {"type": "object", "properties": {}},
        execute=execute,
        callers=tuple(callers),
        replay=replay,
        deferred=deferred,
        structured_output_schema=structured_output_schema,
    )


def section(name: str, render: Callable[[Mapping[str, Any]], str | None], *, tag: bool = True) -> Section:
    return Section(name=name, render=render, tag=tag)


def define_extension(
    *,
    name: str,
    tools: Sequence[Tool] = (),
    sections: Sequence[Section] = (),
    tasks: Sequence[Task] = (),
    hooks: Sequence[Any] = (),
) -> Extension:
    if not name:
        raise TypeError("Extension name must be a non-empty string")
    return Extension(
        name=name, tools=tuple(tools), sections=tuple(sections), tasks=tuple(tasks), hooks=tuple(hooks)
    )


@dataclass(frozen=True)
class RegistrySnapshot:
    extensions: tuple[Extension, ...]
    tasks: dict[str, Task]
    tools: dict[str, Tool]


class Registry:
    """Application-owned registry of installed extensions."""

    def __init__(self) -> None:
        # Imported lazily: the built-in definitions import agent/docs, which import this module.
        from .generation import GenerationTask, ToolTask

        self._extensions: list[Extension] = []
        self._tasks: dict[str, Task] = {}
        self._builtins: dict[str, Task] = {
            GenerationTask.definition.name: GenerationTask,
            ToolTask.definition.name: ToolTask,
        }

    def install(self, extension: Extension) -> None:
        existing = next((item for item in self._extensions if item.name == extension.name), None)
        if existing is not None:
            self._extensions.remove(existing)
        self._extensions.append(extension)
        for task in extension.tasks:
            self._tasks[task.definition.name] = task

    def snapshot(self) -> RegistrySnapshot:
        tasks = {**self._builtins, **self._tasks}
        tools: dict[str, Tool] = {}
        for extension in self._extensions:
            for tool in extension.tools:
                tools[tool.name] = tool
        for name in self._builtins:
            if name not in tasks:
                raise RuntimeError(f"Built-in task {name} is missing")
        return RegistrySnapshot(tuple(self._extensions), tasks, tools)


def create_registry() -> Registry:
    """A registry that always holds the built-in task definitions."""
    return Registry()


__all__ = [
    "Extension",
    "Registry",
    "RegistrySnapshot",
    "Section",
    "Tool",
    "create_registry",
    "define_extension",
    "define_tool",
    "section",
]
