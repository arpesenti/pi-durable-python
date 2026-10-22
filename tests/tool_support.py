"""Shared helpers for the translated ``harness-tools`` suites."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast

from chat_support import (
    FauxModels,
    all_entries,
    assistant_message,
    chat_setup,
    faux_text,
    faux_tool_call,
    open_chat,
)
from session_support import context

from pi_durable import MemoryStorage, ToolResultEntry
from pi_durable.harness.define import define_extension, define_tool, hook
from pi_durable.harness.types import Extension, Registry, ToolRegistration
from pi_durable.types import EntryRecord

ECHO_PARAMETERS = {"type": "object", "properties": {"text": {"type": "string"}}}

#: A plain final answer.
DONE = assistant_message([faux_text("done")])

_hook_counter = 0


def tool(
    name: str,
    execute: Callable[..., Any],
    **extra: Any,
) -> ToolRegistration:
    return define_tool(
        {
            "name": name,
            "description": f"The {name} tool",
            "parameters": ECHO_PARAMETERS,
            "execute": execute,
            **extra,
        }
    )


def calls(*specs: Sequence[Any]) -> dict[str, Any]:
    """A tool-calling answer with one call per ``(name, args, id)``."""

    return assistant_message(
        [faux_tool_call(name, args, id=call_id) for name, args, call_id in specs],
        stop_reason="toolUse",
    )


def results(entries: Sequence[EntryRecord]) -> list[Mapping[str, Any]]:
    return [
        cast(Mapping[str, Any], entry["model"][0])
        for entry in entries
        if ToolResultEntry.is_(entry)
    ]


def result_text(message: Mapping[str, Any] | None) -> str:
    if message is None:
        return ""
    return "|".join(
        cast(str, item.get("text")) if item.get("type") == "text" else f"[{item.get('type')}]"
        for item in cast(Sequence[Mapping[str, Any]], message.get("content") or ())
    )


def add_tool(registry: Registry, registration: ToolRegistration) -> Extension:
    """Install a one-tool extension named after the tool; ``registry.uninstall`` removes it."""

    extension = define_extension({"name": f"tool:{registration.name}", "tools": [registration]})
    registry.install(extension)
    return extension


def add_hooks(registry: Registry, task: Any, handlers: Mapping[str, Any]) -> Extension:
    """Install a one-hook extension for ``task``, as upstream's ``addHooks`` does."""

    global _hook_counter
    _hook_counter += 1
    extension = define_extension(
        {"name": f"hooks:{task.definition.name}:{_hook_counter}", "hooks": [hook(task, handlers)]}
    )
    registry.install(extension)
    return extension


def add_task(registry: Registry, task: Any) -> Extension:
    extension = define_extension({"name": f"task:{task.definition.name}", "tasks": [task]})
    registry.install(extension)
    return extension


async def run(
    setup: Any,
    responses: Sequence[Any],
    prepare: Callable[[Any, Any], Any] | None = None,
    options: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Open a Harness, run one input through ``responses``, and return its entries and status."""

    setup.faux.set_responses(responses)
    opened = await open_chat(MemoryStorage(), setup, **dict(options or {}))
    harness, root = opened["harness"], opened["root"]
    if prepare is not None:
        await prepare(harness, root)
    harness.resume()
    settled = await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    return {
        "harness": harness,
        "root": root,
        "entries": await all_entries(root),
        "status": settled["status"],
    }


__all__ = [
    "DONE",
    "ECHO_PARAMETERS",
    "FauxModels",
    "add_hooks",
    "add_task",
    "add_tool",
    "all_entries",
    "assistant_message",
    "calls",
    "chat_setup",
    "context",
    "faux_text",
    "open_chat",
    "result_text",
    "results",
    "run",
    "tool",
]
