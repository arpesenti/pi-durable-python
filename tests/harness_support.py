"""Translated v1.3.0 ``harness-support`` message helpers.

Only the message constructors and rendering used by the context suite are ported so far; the
registry and Harness helpers arrive with the Harness tickets.
"""

from __future__ import annotations

from typing import Any, cast

from pi_durable.types import MessageRecord


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


__all__ = ["assistant", "describe_message", "system", "tool_result", "user"]
