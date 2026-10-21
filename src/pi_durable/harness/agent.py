"""Per-conversation agent state and resolution against a registry snapshot.

The stored agent document holds names, never code: model, thinking level, extension and
tool names, instructions, and cwd. Resolution skips names this process cannot execute.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ..context import system_message
from ..json import copy_json
from .docs import AgentDoc, normalize_tool_reference
from .registry import Extension, RegistrySnapshot, Section, Tool


@dataclass(frozen=True)
class Agent:
    model: dict[str, Any] | None
    thinking_level: str
    extensions: tuple[Extension, ...]
    tools: tuple[Tool, ...]
    callable_tools: tuple[Tool, ...]
    deferred: tuple[Tool, ...]
    sections: tuple[Section, ...]
    instructions: str | None
    cwd: str | None
    state: dict[str, Any] = field(default_factory=dict)


def _names(values: Sequence[Any]) -> list[str]:
    names: list[str] = []
    for value in values:
        if isinstance(value, str):
            names.append(value)
        elif isinstance(value, Mapping) and isinstance(value.get("name"), str):
            names.append(value["name"])
        elif hasattr(value, "definition") and hasattr(value, "kind"):
            names.append(value.kind)
        elif hasattr(value, "name"):
            names.append(value.name)
    return names


def resolve_agent(snapshot: RegistrySnapshot, state: Mapping[str, Any], settings: Any) -> Agent:
    installed = {extension.name: extension for extension in snapshot.extensions}
    default_extensions: list[Extension]
    configured_default = getattr(settings, "extensions", None)
    if configured_default is None:
        default_extensions = list(snapshot.extensions)
    else:
        default_extensions = [item for item in configured_default if isinstance(item, Extension)]
        if not default_extensions:
            default_extensions = [installed[name] for name in _names(configured_default) if name in installed]
    selection = state.get("extensions")
    selected = {extension.name: extension for extension in default_extensions}
    if isinstance(selection, list):
        extensions = [installed[name] for name in _names(selection) if name in installed]
    else:
        if isinstance(selection, Mapping):
            for name in _names(selection.get("remove") or []):
                selected.pop(name, None)
            for name in _names(selection.get("add") or []):
                if name in installed:
                    selected[name] = installed[name]
        extensions = [extension for extension in default_extensions if extension.name in selected]
        for extension in snapshot.extensions:
            if extension.name in selected and extension not in extensions:
                extensions.append(extension)

    enabled: dict[str, Tool] = {}
    for extension in extensions:
        for tool in extension.tools:
            enabled[tool.name] = tool
    tool_filter = state.get("tools")
    if isinstance(tool_filter, list):
        ordered = [enabled[name] for name in _names(tool_filter) if name in enabled]
    elif isinstance(tool_filter, Mapping):
        removed = set(_names(tool_filter.get("remove") or []))
        ordered = [tool for name, tool in enabled.items() if name not in removed]
    else:
        ordered = list(enabled.values())

    callable_tools = [tool for tool in ordered if "tools" in tool.callers]
    deferred = [tool for tool in ordered if tool.deferred and "model" in tool.callers]
    model_filter = state.get("modelTools")
    if isinstance(model_filter, list):
        offered = [tool for name in _names(model_filter) for tool in ordered if tool.name == name]
    elif isinstance(model_filter, Mapping):
        removed = set(_names(model_filter.get("remove") or []))
        offered = [
            tool for tool in ordered if "model" in tool.callers and not tool.deferred and tool.name not in removed
        ]
        for name in _names(model_filter.get("add") or []):
            extra = enabled.get(name)
            if extra is not None and extra.name not in removed and extra not in offered:
                offered.append(extra)
    else:
        offered = [tool for tool in ordered if "model" in tool.callers and not tool.deferred]

    sections: list[Section] = []
    for extension in extensions:
        sections.extend(extension.sections)
    instructions = state.get("instructions")
    model = state.get("model")
    return Agent(
        model=copy_json(model) if isinstance(model, Mapping) else None,
        thinking_level=str(state.get("thinkingLevel") or "off"),
        extensions=tuple(extensions),
        tools=tuple(offered),
        callable_tools=tuple(callable_tools),
        deferred=tuple(deferred),
        sections=tuple(sections),
        instructions=instructions if isinstance(instructions, str) else None,
        cwd=state.get("cwd") if isinstance(state.get("cwd"), str) else None,
        state=copy_json(dict(state)),
    )


async def render_system_message(
    agent: Agent,
    messages: Sequence[Mapping[str, Any]],
    *,
    context: Mapping[str, Any] | None = None,
    timestamp: int = 0,
    report: Any = None,
) -> dict[str, Any] | None:
    """Compare desired prompt and tools with the replayed system state; return a delta or None."""
    current_sections: dict[str, str] = {}
    current_tools: dict[str, dict[str, Any]] = {}
    for message in messages:
        if message.get("role") != "system":
            continue
        for name, value in (message.get("sections") or {}).items():
            if value is None:
                current_sections.pop(name, None)
            else:
                current_sections[name] = value
        for reference in message.get("toolsRemoved") or []:
            name = normalize_tool_reference(reference)
            if name is not None:
                current_tools.pop(name, None)
        for reference in message.get("toolsAdded") or []:
            name = normalize_tool_reference(reference)
            if name is not None:
                current_tools[name] = copy_json(reference)

    desired_sections: dict[str, str] = {}
    render_context = dict(context or {})
    for section in agent.sections:
        try:
            rendered = section.render(render_context)
        except BaseException as error:  # noqa: BLE001 - a section failure is reported, not fatal
            if report is not None:
                report(error, {"type": "extension_error", "extension": section.name})
            rendered = None
        if isinstance(rendered, str) and rendered:
            desired_sections[section.name] = rendered
    if agent.instructions:
        desired_sections["instructions"] = agent.instructions
    desired_tools = {tool.name: tool.declaration for tool in agent.tools}

    section_patch: dict[str, str | None] = {}
    for name, value in desired_sections.items():
        if current_sections.get(name) != value:
            section_patch[name] = value
    for name in current_sections:
        if name not in desired_sections:
            section_patch[name] = None
    removed: list[Any] = []
    added: list[dict[str, Any]] = []
    for name, declaration in current_tools.items():
        desired = desired_tools.get(name)
        if desired is None or desired != declaration:
            removed.append(name)
    for name, declaration in desired_tools.items():
        if current_tools.get(name) != declaration:
            added.append(declaration)
    if not section_patch and not removed and not added:
        return None
    return system_message(
        section_patch or None,
        tools_added=added or None,
        tools_removed=removed or None,
        timestamp=timestamp,
    )


async def configure(tx: Any, conversation_id: int, change: Mapping[str, Any]) -> None:
    """Apply one agent change inside a transaction."""
    doc = await tx.doc(AgentDoc, conversation_id)
    for key in ("model", "thinkingLevel", "instructions", "cwd"):
        if key not in change:
            continue
        value = change[key]
        if value is None:
            doc.pop(key, None)
        else:
            doc[key] = copy_json(value)
    if "extensions" in change:
        value = change["extensions"]
        if value is None:
            doc.pop("extensions", None)
        elif isinstance(value, Mapping):
            doc["extensions"] = {
                "add": _names(value.get("add") or []),
                "remove": _names(value.get("remove") or []),
            }
        else:
            doc["extensions"] = _names(value)
    if "tools" in change:
        value = change["tools"]
        if value is None:
            doc.pop("tools", None)
        elif isinstance(value, Mapping):
            doc["tools"] = {"remove": _names(value.get("remove") or [])}
        else:
            doc["tools"] = _names(value)
    if "modelTools" in change:
        value = change["modelTools"]
        if value is None:
            doc.pop("modelTools", None)
        elif isinstance(value, Mapping):
            doc["modelTools"] = {
                **({"remove": _names(value["remove"])} if value.get("remove") else {}),
                **({"add": _names(value["add"])} if value.get("add") else {}),
            }
        else:
            doc["modelTools"] = _names(value)


async def read_agent_state(session: Any, conversation_id: int) -> dict[str, Any]:
    state = await session.snapshot(AgentDoc, conversation_id)
    return copy_json(state) if state is not None else {}


__all__ = [
    "Agent",
    "configure",
    "read_agent_state",
    "render_system_message",
    "resolve_agent",
]
