"""Agent state, settings, and registry-backed agent resolution."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, cast

from ..documents import define_doc
from ..session.transaction import Transaction
from ..types import ConversationId, ConversationRecord
from .types import (
    Agent,
    Extension,
    PromptSection,
    RegistrySnapshot,
    Settings,
    ToolRegistration,
)

INSTRUCTIONS_KEY = "instructions"

DEFAULT_RETRY_POLICY: dict[str, Any] = {
    "enabled": True,
    "max_retries": 3,
    "base_delay_ms": 2_000,
    "max_agent_delay_ms": 60_000,
}

DEFAULT_COMPACTION_POLICY: dict[str, Any] = {
    "enabled": True,
    "reserve_tokens": 16_384,
    "keep_recent_tokens": 20_000,
    "background_tokens": 32_768,
}

DEFAULT_PROGRESS_POLICY: dict[str, Any] = {
    "partial_interval_ms": 100,
    "output_interval_ms": 100,
}

DEFAULT_CONTEXT_RETENTION_MS = 600_000

AgentDoc = define_doc(
    {
        "kind": "pi.agent",
        "version": 1,
        "scope": "conversation",
        "history": "rewindable",
        "fork": "asOf",
        "initial": lambda: {},
        "checkpoint_when": lambda _state, _ops, _info: True,
    }
)


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def resolve_settings(settings: Any) -> Settings:
    """Resolve the host settings: every field over its built-in default, object fields merged."""

    extensions = _field(settings, "extensions")
    merged_retry = dict(DEFAULT_RETRY_POLICY)
    merged_retry.update(_field(settings, "retry", {}) or {})
    merged_compaction = dict(DEFAULT_COMPACTION_POLICY)
    merged_compaction.update(_field(settings, "compaction", {}) or {})
    partial = _field(settings, "progress", {}) or {}
    progress = dict(DEFAULT_PROGRESS_POLICY)
    for key in progress:
        value = _field(partial, key)
        if value is not None:
            progress[key] = value
    return Settings(
        extensions=extensions,
        stream=dict(_field(settings, "stream", {}) or {}),
        retry=merged_retry,
        compaction=merged_compaction,
        progress=progress,
        tool_execution=_field(settings, "tool_execution", "parallel"),
        steering_mode=_field(settings, "steering_mode", "one-at-a-time"),
        follow_up_mode=_field(settings, "follow_up_mode", "one-at-a-time"),
        context_retention_ms=_field(
            settings, "context_retention_ms", DEFAULT_CONTEXT_RETENTION_MS
        ),
        document_retention_ms=_field(settings, "document_retention_ms", 600_000),    )


async def configure(tx: Transaction, conversation_id: ConversationId, change: Mapping[str, Any]) -> None:
    """Apply one change to ``pi.agent``: a given field replaces the stored one, ``None`` clears it."""

    state = await tx.doc(AgentDoc, conversation_id)
    for key, value in change.items():
        field = _CHANGE_FIELDS.get(key, key)
        if field not in _AGENT_FIELDS:
            continue
        if value is None:
            state.pop(field, None)
        elif field in ("extensions", "tools", "modelTools"):
            state[field] = _names(value)
        else:
            state[field] = value


_CHANGE_FIELDS = {
    "thinking_level": "thinkingLevel",
    "model_tools": "modelTools",
}

_AGENT_FIELDS = frozenset(
    {"model", "thinkingLevel", "extensions", "tools", "modelTools", "instructions", "cwd"}
)


def _names(value: Any) -> Any:
    """Store names, not objects, for an agent selection change; a dict keeps its list fields."""

    if isinstance(value, Mapping):
        return {key: [_name(item) for item in items] for key, items in value.items()}
    if isinstance(value, (list, tuple)):
        return [_name(item) for item in value]
    return value


def _name(item: Any) -> Any:
    return getattr(item, "name", item)


async def create_agent(tx: Transaction, conversation: ConversationRecord) -> None:
    """Built-in part of every commit that creates a conversation: copy the owner's agent state."""

    if "parent" in conversation:
        return
    state = await tx.doc(AgentDoc, conversation["id"])
    owner = conversation.get("owner")
    if owner is None:
        return
    owner_state = await tx.doc(AgentDoc, owner["conversationId"])
    state.update(cast(Mapping[str, Any], owner_state))


def agent_hooks(agent: Agent, task_name: str) -> list[Mapping[str, Any]]:
    """Handlers of the selected extensions' hooks for a task name, in extension order."""

    return [
        hook.handlers for extension in agent.extensions for hook in extension.hooks if hook.task == task_name
    ]


def resolve_agent(
    state: Mapping[str, Any] | None,
    snapshot: RegistrySnapshot,
    settings: Settings,
    report: Callable[[BaseException], None],
) -> Agent:
    """Resolve an agent from its stored state, a registry snapshot, and resolved settings."""

    stored = dict(state or {})
    extensions = _select_extensions(stored.get("extensions"), snapshot, settings)

    composed: dict[str, ToolRegistration] = {}
    for extension in extensions:
        for tool in extension.tools:
            composed[tool.name] = tool
    sections: dict[str, PromptSection] = {}
    for extension in extensions:
        for section in extension.sections:
            sections[section.key] = section
    for extension in extensions:
        for wrap in extension.wraps:
            if wrap.tool is not None and wrap.wrap is not None:
                _apply_wrap(composed, wrap.tool, wrap.wrap, lambda tool: tool.name, report)
            elif wrap.section is not None and wrap.wrap is not None:
                _apply_wrap(sections, wrap.section, wrap.wrap, lambda section: section.key, report)

    enabled = _filter_tools(list(composed.values()), stored.get("tools"))
    model_callable = [tool for tool in enabled if tool.callers is None or "model" in tool.callers]
    tools = _offered_tools(model_callable, stored.get("modelTools"))
    offered = {tool.name for tool in tools}
    deferred = [tool for tool in model_callable if tool.deferred and tool.name not in offered]
    callable_tools = [tool for tool in enabled if tool.callers is None or "tools" in tool.callers]

    instructions = stored.get("instructions")
    agent_sections = list(sections.values())
    if instructions is not None:
        agent_sections.append(
            PromptSection(key=INSTRUCTIONS_KEY, render=lambda *_args: instructions)
        )

    return Agent(
        model=stored.get("model"),
        thinking_level=stored.get("thinkingLevel", "off"),
        extensions=extensions,
        tools=tools,
        callable=callable_tools,
        deferred=deferred,
        sections=agent_sections,
        instructions=instructions,
        cwd=stored.get("cwd"),
    )


def _apply_wrap(
    items: dict[str, Any],
    target: str,
    wrap: Callable[..., Any],
    name_of: Callable[[Any], str],
    report: Callable[[BaseException], None],
) -> None:
    item = items.get(target)
    if item is None:
        return
    try:
        wrapped = wrap(item)
        renamed = name_of(wrapped)
        if renamed != target:
            raise ValueError(f"Wrapper renamed {target} to {renamed}")
        items[target] = wrapped
    except Exception as error:
        items.pop(target, None)
        report(error)


def _offered_tools(
    tools: list[ToolRegistration],
    filter_value: Any,
) -> list[ToolRegistration]:
    if isinstance(filter_value, (list, tuple)):
        return _filter_tools(tools, filter_value)
    removed = _name_set(_field(filter_value, "remove", []))
    kept = [tool for tool in tools if not tool.deferred and tool.name not in removed]
    by_name = {tool.name: tool for tool in tools}
    loaded: list[ToolRegistration] = []
    for name in _unique_names(_field(filter_value, "add", [])):
        tool = by_name.get(name)
        if tool is not None and tool.deferred and name not in removed:
            loaded.append(tool)
    return [*kept, *loaded]


def _filter_tools(tools: list[ToolRegistration], filter_value: Any) -> list[ToolRegistration]:
    if filter_value is None:
        return tools
    if isinstance(filter_value, (list, tuple)):
        by_name = {tool.name: tool for tool in tools}
        return [by_name[name] for name in _unique_names(filter_value) if name in by_name]
    removed = _name_set(_field(filter_value, "remove", []))
    return [tool for tool in tools if tool.name not in removed]


def _unique_names(items: Any) -> list[str]:
    """Names in order, a repeated name only at its first position."""

    names: list[str] = []
    seen: set[str] = set()
    for item in items:
        name = _name(item)
        if name not in seen:
            seen.add(name)
            names.append(name)
    return names


def _name_set(items: Any) -> set[str]:
    return {_name(item) for item in items}


def _select_extensions(
    stored: Any,
    snapshot: RegistrySnapshot,
    settings: Settings,
) -> list[Extension]:
    if isinstance(stored, (list, tuple)):
        selected = _unique_names(stored)
    else:
        if settings.extensions is not None:
            base = _unique_names(settings.extensions)
        else:
            base = _unique_names(snapshot.installed())
        edit = stored if isinstance(stored, Mapping) else {}
        removed = _name_set(edit.get("remove", []))
        selected = [
            name
            for name in _unique_names([*base, *edit.get("add", [])])
            if name not in removed
        ]
    extensions: list[Extension] = []
    for name in selected:
        extension = snapshot.extension(name)
        if extension is not None:
            extensions.append(extension)
    return extensions


__all__ = [
    "AgentDoc",
    "DEFAULT_COMPACTION_POLICY",
    "DEFAULT_CONTEXT_RETENTION_MS",
    "DEFAULT_PROGRESS_POLICY",
    "DEFAULT_RETRY_POLICY",
    "INSTRUCTIONS_KEY",
    "agent_hooks",
    "configure",
    "create_agent",
    "resolve_agent",
    "resolve_settings",
]
