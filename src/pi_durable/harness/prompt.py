"""System prompt replay, rendering, and positional ``pi.system`` planning.

Port of v1.3.0 ``src/harness/prompt.ts``. Durable stores no prompt sections of its own: the
conversation's resolved agent renders the desired sections for each request, and every change is
written as a positional ``pi.system`` entry whose model message is a pi-ai ``SystemMessage``.
Replaying those messages in transcript order yields the sections and tools the model has seen.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, Never, NotRequired, TypedDict, cast

from ..context import Context
from ..entries import SystemEntry
from ..json import JsonValue, copy_json
from ..types import ContextEdit, MessageRecord, TypedEntryDraft
from .types import ContextView, PromptInput, PromptSection, ToolRegistration


class ToolDeclaration(TypedDict):
    """The provider-facing part of a tool, exactly as persisted in a system message."""

    name: str
    description: str
    parameters: JsonValue
    constrainedSampling: NotRequired[JsonValue]


class ToolChanges(TypedDict):
    """Tool removals and additions planned for one positional message."""

    toolsRemoved: list[dict[str, str]]
    toolsAdded: list[ToolDeclaration]


SystemDraft = TypedEntryDraft[Never]


def replay_sections(messages: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    """Replay system messages into the sections in effect.

    A section sets or replaces its name in place without moving it, ``null`` removes it, and a
    later re-addition appends it to the ordered map. Instruction ``content`` is not a section:
    durable always writes an empty ``content``, and the messages keep it for provider replay.
    """

    shown: dict[str, str] = {}
    for message in messages:
        if message.get("role") != "system":
            continue
        sections = message.get("sections")
        if sections is None:
            continue
        for key, value in cast(Mapping[str, str | None], sections).items():
            if value is None:
                shown.pop(key, None)
            else:
                shown[key] = value
    return shown


async def render_sections(
    sections: Sequence[PromptSection],
    input: PromptInput,
    shown: Mapping[str, str],
    report: Callable[[BaseException], None],
    context: Context,
) -> dict[str, str]:
    """Render the agent's sections in order.

    A renderer returning ``None`` omits its section. Tagged text is wrapped as
    ``<key>\\n...\\n</key>``. A section that raises keeps its shown text, if any, and is reported;
    errors after the invocation was cancelled propagate.
    """

    desired: dict[str, str] = {}
    for section in sections:
        try:
            rendered = section.render(input, context)
            if hasattr(rendered, "__await__"):
                rendered = await cast(Awaitable[Any], rendered)
        except Exception as error:
            signal = context.abort_signal
            if signal is not None and signal.aborted:
                raise
            report(error)
            kept = shown.get(section.key)
            if kept is not None:
                desired[section.key] = kept
            continue
        if rendered is None:
            continue
        text = cast(str, rendered)
        desired[section.key] = (
            text if section.tag is False else f"<{section.key}>\n{text}\n</{section.key}>"
        )
    return desired


def to_tool_declaration(
    tool: ToolRegistration | Mapping[str, Any],
) -> ToolDeclaration:
    """Strip execution and display-only fields, leaving the declaration the model sees."""

    if isinstance(tool, Mapping):
        name = cast(str, tool["name"])
        description = cast(str, tool.get("description") or "")
        parameters = tool.get("parameters")
        sampling = tool.get("constrainedSampling")
    else:
        name = tool.name
        description = tool.description
        parameters = tool.parameters
        sampling = tool.constrained_sampling
    declaration: dict[str, Any] = {
        "name": name,
        "description": description,
        "parameters": copy_json(parameters),
    }
    if sampling is not None:
        declaration["constrainedSampling"] = copy_json(sampling)
    return cast(ToolDeclaration, declaration)


def declarations_equal(
    left: ToolRegistration | Mapping[str, Any],
    right: ToolRegistration | Mapping[str, Any],
) -> bool:
    """Whether two tools declare the same interface to the model."""

    return _serialized(left) == _serialized(right)


def _serialized(tool: ToolRegistration | Mapping[str, Any]) -> str:
    return json.dumps(to_tool_declaration(tool), ensure_ascii=False, separators=(",", ":"))


def current_tools(messages: Sequence[Mapping[str, Any]]) -> list[ToolDeclaration]:
    """Resolve the tools available after applying every transcript delta in order."""

    tools: dict[str, ToolDeclaration] = {}
    for message in messages:
        if message.get("role") != "system":
            continue
        for reference in cast(Sequence[Mapping[str, Any]], message.get("toolsRemoved") or []):
            tools.pop(cast(str, reference["name"]), None)
        for tool in cast(Sequence[Mapping[str, Any]], message.get("toolsAdded") or []):
            tools[cast(str, tool["name"])] = cast(ToolDeclaration, dict(tool))
    return list(tools.values())


def plan_system_entries(
    view: ContextView,
    desired: Mapping[str, str],
    tools: Sequence[ToolRegistration | Mapping[str, Any]],
    timestamp: int,
) -> list[SystemDraft]:
    """Plan the ``pi.system`` entries that make ``view`` equal ``desired`` in values and order.

    - A head marker with no later ``pi.system`` entry in context: one complete baseline that
      omits every retained earlier ``pi.system`` entry, written even when it restates replay.
    - Otherwise, when a minimal section patch would leave a different order: remove every shown
      section, then re-add every desired section in order.
    - Otherwise the minimal patch of changed values and ``null`` removals, or nothing.

    Tool changes ride on the last planned entry, or on one entry of their own.
    """

    head = view.get("head")
    if head is not None and not any(
        SystemEntry.is_(entry) and entry["id"] > head["id"] for entry in view["entries"]
    ):
        edits = [
            cast(ContextEdit, {"target": entry["id"], "action": "omit"})
            for entry in view["entries"]
            if SystemEntry.is_(entry)
        ]
        baseline = _system_entry(
            dict(desired),
            {
                "toolsRemoved": [],
                "toolsAdded": [to_tool_declaration(tool) for tool in tools],
            },
            timestamp,
        )
        if edits:
            baseline["edits"] = edits
        return [baseline]

    patches = _plan_sections(replay_sections(view["messages"]), desired)
    changes = _plan_tools(current_tools(view["messages"]), tools)
    if not changes["toolsRemoved"] and not changes["toolsAdded"]:
        return [_system_entry(patch, None, timestamp) for patch in patches]
    if not patches:
        return [_system_entry(None, changes, timestamp)]
    return [
        _system_entry(patch, changes if index == len(patches) - 1 else None, timestamp)
        for index, patch in enumerate(patches)
    ]


def _plan_sections(
    shown: Mapping[str, str],
    desired: Mapping[str, str],
) -> list[dict[str, str | None]]:
    patched_order = [
        *[key for key in shown if key in desired],
        *[key for key in desired if key not in shown],
    ]
    if patched_order != list(desired):
        return [
            {key: None for key in shown},
            dict(desired),
        ]

    patch: dict[str, str | None] = {}
    for key, value in shown.items():
        next_value = desired.get(key)
        if next_value != value:
            patch[key] = next_value
    for key, value in desired.items():
        if key not in shown:
            patch[key] = value
    return [] if not patch else [patch]


def _plan_tools(
    offered: Sequence[ToolDeclaration],
    desired: Sequence[ToolRegistration | Mapping[str, Any]],
) -> ToolChanges:
    declared = [to_tool_declaration(tool) for tool in desired]
    wanted = {tool["name"]: tool for tool in declared}
    kept = [
        tool
        for tool in offered
        if (next_tool := wanted.get(tool["name"])) is not None
        and declarations_equal(tool, next_tool)
    ]
    kept_names = {tool["name"] for tool in kept}
    added = [tool for tool in declared if tool["name"] not in kept_names]
    replayed = [*kept, *added]
    if len(replayed) != len(declared) or any(
        tool["name"] != declared[index]["name"] for index, tool in enumerate(replayed)
    ):
        return {
            "toolsRemoved": [{"name": tool["name"]} for tool in offered],
            "toolsAdded": declared,
        }
    return {
        "toolsRemoved": [
            {"name": tool["name"]} for tool in offered if tool["name"] not in kept_names
        ],
        "toolsAdded": added,
    }


def _system_entry(
    sections: Mapping[str, str | None] | None,
    tools: ToolChanges | None,
    timestamp: int,
) -> SystemDraft:
    message: dict[str, Any] = {"role": "system", "content": ""}
    if sections is not None:
        message["sections"] = dict(sections)
    if tools is not None:
        if tools["toolsRemoved"]:
            message["toolsRemoved"] = tools["toolsRemoved"]
        if tools["toolsAdded"]:
            message["toolsAdded"] = tools["toolsAdded"]
    message["timestamp"] = timestamp
    return cast(
        SystemDraft,
        {
            "kind": SystemEntry.kind,
            "model": cast(list[MessageRecord], [message]),
        },
    )


__all__ = [
    "SystemDraft",
    "ToolChanges",
    "ToolDeclaration",
    "current_tools",
    "declarations_equal",
    "plan_system_entries",
    "render_sections",
    "replay_sections",
    "to_tool_declaration",
]
