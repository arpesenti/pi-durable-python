"""Typed constructors for extensions, tools, sections, and hooks."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from .types import (
    AnyTask,
    Extension,
    HookRegistration,
    PromptSection,
    ToolRegistration,
    Wrap,
)


def _values(value: Mapping[str, Any] | None, kwargs: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(value or {})
    result.update(kwargs)
    return result


def define_extension(
    extension: Mapping[str, Any] | None = None,
    /,
    **kwargs: Any,
) -> Extension:
    """Identity constructor that types an extension."""

    values = _values(extension, kwargs)
    name = values.get("name")
    if not isinstance(name, str) or not name:
        raise TypeError("Extension name must be a non-empty string")
    return Extension(
        name=name,
        tools=tuple(values.get("tools") or ()),
        sections=tuple(values.get("sections") or ()),
        hooks=tuple(values.get("hooks") or ()),
        wraps=tuple(values.get("wraps") or ()),
        tasks=tuple(values.get("tasks") or ()),
    )


def define_tool(
    tool: Mapping[str, Any] | None = None,
    /,
    **kwargs: Any,
) -> ToolRegistration:
    """Identity constructor that types a tool."""

    values = _values(tool, kwargs)
    name = values.get("name")
    if not isinstance(name, str) or not name:
        raise TypeError("Tool name must be a non-empty string")
    return ToolRegistration(
        name=name,
        description=str(values.get("description") or ""),
        parameters=values.get("parameters"),
        execute=values.get("execute"),
        replay=values.get("replay"),
        execution_mode=values.get("execution_mode"),
        structured_output_schema=values.get("structured_output_schema"),
        callers=values.get("callers"),
        deferred=values.get("deferred") is True,
        output_limits=values.get("output_limits"),
        constrained_sampling=values.get("constrained_sampling"),
    )


def section(
    key: str,
    render: Callable[..., Any],
    options: Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> PromptSection:
    """Create a prompt section; tagged unless ``tag`` is false."""

    values = {**(options or {}), **kwargs}
    return PromptSection(key=key, render=render, tag=values.get("tag"))


def hook(task: AnyTask, handlers: Mapping[str, Any]) -> HookRegistration:
    """Hook handlers for tasks named like ``task``."""

    return HookRegistration(task=task.definition.name, handlers=dict(handlers))


def wrap_tool(tool: ToolRegistration, wrapper: Callable[[ToolRegistration], ToolRegistration]) -> Wrap:
    """Wrap the tool named like ``tool`` wherever the wrapping extension is selected."""

    return Wrap(tool=tool.name, wrap=wrapper)


def wrap_section(key: str, wrapper: Callable[[PromptSection], PromptSection]) -> Wrap:
    """Wrap the section ``key`` wherever the wrapping extension is selected."""

    return Wrap(section=key, wrap=wrapper)


__all__ = ["define_extension", "define_tool", "hook", "section", "wrap_section", "wrap_tool"]
