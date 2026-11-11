"""Typed constructors for extensions, tools, sections, and hooks."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from .live import COMPACTION_TASK_KIND, GENERATION_TASK_NAME, TOOL_TASK_KIND
from .types import (
    AnyTask,
    CompactionHooks,
    Extension,
    GenerationHooks,
    HookRegistration,
    PromptSection,
    RunEndTask,
    ToolHooks,
    ToolRegistration,
    Wrap,
)

#: The hook names the port's built-in tasks dispatch, by the task name dispatch matches. Each set is the
#: other's hook TypedDict's keys — the one place the exhaustive set is updated — and the ``hooks.each`` sites
#: in ``harness/generation.py``, ``harness/tool.py``, and ``harness/compaction.py`` spell names from it;
#: ``hook()`` refuses a name outside it.
_BUILTIN_TASK_HOOKS = {
    GENERATION_TASK_NAME: frozenset(GenerationHooks.__annotations__),
    TOOL_TASK_KIND: frozenset(ToolHooks.__annotations__),
    COMPACTION_TASK_KIND: frozenset(CompactionHooks.__annotations__),
}


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
        execution_mode=_field(values, "execution_mode", "executionMode"),
        structured_output_schema=_field(
            values, "structured_output_schema", "structuredOutputSchema"
        ),
        callers=values.get("callers"),
        deferred=values.get("deferred") is True,
        output_limits=_field(values, "output_limits", "outputLimits"),
        constrained_sampling=_field(values, "constrained_sampling", "constrainedSampling"),
        prepare_arguments=_field(values, "prepare_arguments", "prepareArguments"),
    )


def _field(values: Mapping[str, Any], name: str, camel: str) -> Any:
    """A Python-facing snake_case field, accepting the upstream camelCase spelling too."""

    value = values.get(name)
    return values.get(camel) if value is None else value


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
    """Hook handlers for tasks named like ``task``.

    A built-in task's hooks are the ones its task's hook map spells, so a name outside that set — a
    typo — is refused here rather than silently never dispatched. A task of its own dispatches
    whatever names it asks and keeps them.
    """

    name = task.definition.name
    allowed = _BUILTIN_TASK_HOOKS.get(name)
    if allowed is not None:
        for handler_name in handlers:
            if handler_name not in allowed:
                raise TypeError(f"Task {name} has no hook named {handler_name!r}")
    return HookRegistration(task=name, handlers=dict(handlers))


def run_end_task(task: AnyTask, input: Any) -> RunEndTask:
    """A task an ``afterRun`` hook asks the generation to run, with the input it starts on."""

    return {"task": task, "input": input}


def wrap_tool(tool: ToolRegistration, wrapper: Callable[[ToolRegistration], ToolRegistration]) -> Wrap:
    """Wrap the tool named like ``tool`` wherever the wrapping extension is selected."""

    return Wrap(tool=tool.name, wrap=wrapper)


def wrap_section(key: str, wrapper: Callable[[PromptSection], PromptSection]) -> Wrap:
    """Wrap the section ``key`` wherever the wrapping extension is selected."""

    return Wrap(section=key, wrap=wrapper)


__all__ = [
    "define_extension",
    "define_tool",
    "hook",
    "run_end_task",
    "section",
    "wrap_section",
    "wrap_tool",
]
