"""Translated v1.3.0 ``harness-support`` message, registry, and Harness helpers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from session_support import context

from pi_durable import MemoryStorage
from pi_durable.harness.define import define_extension, define_tool, section
from pi_durable.harness.harness import Harness
from pi_durable.harness.registry import create_registry
from pi_durable.harness.types import Extension, Registry, ToolRegistration
from pi_durable.types import MessageRecord, Storage


def tool(name: str, description: str | None = None) -> ToolRegistration:
    return define_tool(
        {
            "name": name,
            "description": f"{name} tool" if description is None else description,
            "parameters": {"type": "object", "properties": {}},
            "execute": lambda *_args: None,
        }
    )


async def open_harness(
    storage: Storage | None = None,
    tool_names: tuple[str, ...] = (),
    *,
    registry: Registry | None = None,
    on_report: Callable[[BaseException], Any] | None = None,
) -> dict[str, Any]:
    """Open a Harness with a fresh registry holding the named tools."""

    selected = registry or create_registry()
    if tool_names:
        selected.install(define_extension({"name": "tools", "tools": [tool(name) for name in tool_names]}))
    harness = await Harness.open(
        storage or MemoryStorage(),
        {
            "models": {},
            "registry": selected,
            **({} if on_report is None else {"on_report": on_report}),
        },
        context,
    )
    return {"harness": harness, "registry": selected}


def add_section(
    registry: Registry,
    key: str,
    render: Callable[..., Any],
    options: dict[str, Any] | None = None,
    name: str | None = None,
) -> Extension:
    """Install a one-section extension named after the section."""

    extension = define_extension(
        {
            "name": f"section:{key}" if name is None else name,
            "sections": [section(key, render, options)],
        }
    )
    registry.install(extension)
    return extension


def user(text: str) -> MessageRecord:
    return {"role": "user", "content": text, "timestamp": 1}


def assistant(
    text: str,
    options: dict[str, Any] | None = None,
) -> MessageRecord:
    values = options or {}
    calls = [
        {"type": "toolCall", "id": call_id, "name": f"tool-{call_id}", "arguments": {}}
        for call_id in values.get("calls") or []
    ]
    stop_reason = values.get("stop_reason") or ("toolUse" if calls else "stop")
    return {
        "role": "assistant",
        "content": [{"type": "text", "text": text}, *calls],
        "api": "faux",
        "provider": "faux",
        "model": "faux",
        "usage": {
            "input": 0,
            "output": 0,
            "cacheRead": 0,
            "cacheWrite": 0,
            "totalTokens": 0,
            "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0},
        },
        "stopReason": stop_reason,
        "timestamp": 2,
    }


def tool_result(tool_call_id: str, text: str | None = None) -> MessageRecord:
    return {
        "role": "toolResult",
        "toolCallId": tool_call_id,
        "toolName": f"tool-{tool_call_id}",
        "content": [{"type": "text", "text": f"result {tool_call_id}" if text is None else text}],
        "isError": False,
        "timestamp": 3,
    }


def system(sections: dict[str, str | None]) -> MessageRecord:
    return cast(MessageRecord, {"role": "system", "content": "", "sections": sections, "timestamp": 4})


def describe_message(message: MessageRecord) -> str:
    """Render one message compactly for assertions."""

    value = cast(dict[str, Any], message)
    role = value["role"]
    if role == "user":
        return f"user:{value['content']}"
    if role == "assistant":
        content = cast(list[dict[str, Any]], value["content"])
        text = next((item for item in content if item.get("type") == "text"), None)
        return f"assistant:{text['text'] if text is not None else ''}"
    if role == "toolResult":
        if value.get("isError"):
            return f"result:{value['toolCallId']}:error"
        content = cast(list[dict[str, Any]], value["content"])
        text = next((item for item in content if item.get("type") == "text"), None)
        return f"result:{value['toolCallId']}:{text['text'] if text is not None else ''}"
    if role == "system":
        sections = cast(dict[str, Any], value.get("sections") or {})
        return f"system:{','.join(sections.keys())}"
    raise AssertionError(f"Unknown message role {role}")


__all__ = [
    "add_section",
    "assistant",
    "describe_message",
    "open_harness",
    "system",
    "tool",
    "tool_result",
    "user",
]
