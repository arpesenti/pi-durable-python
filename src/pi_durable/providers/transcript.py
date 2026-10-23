"""Provider-facing transcript replay shared by the model adapters.

Port of pi-ai 1.3.0 ``utils/transcript.ts`` and the cross-model parts of
``api/transform-messages.ts``. Durable hands every provider a pi-ai-shaped message list whose system
messages carry the prompt and tool changes. Adapters collapse those deltas into one leading system
message (pi-ai's default for providers without mid-conversation system support), replay tools, drop
thinking signed by another model, and replace images a model cannot take.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast

from . import Model

NON_VISION_USER_IMAGE_PLACEHOLDER = "(image omitted: model does not support images)"
NON_VISION_TOOL_IMAGE_PLACEHOLDER = "(tool image omitted: model does not support images)"


def content_text(content: Any, separator: str = "\n") -> str:
    """Extract and join the text of a message content."""

    if isinstance(content, str):
        return content
    return separator.join(
        cast(str, block.get("text") or "")
        for block in cast(Sequence[Mapping[str, Any]], content or ())
        if block.get("type") == "text"
    )


def system_message_text(message: Mapping[str, Any]) -> str:
    """Render a system message as a complete prompt: its content followed by its sections."""

    parts = [content_text(message.get("content"))]
    for value in cast(Mapping[str, str | None], message.get("sections") or {}).values():
        if value is not None:
            parts.append(value)
    return "\n\n".join(part for part in parts if part)


def current_tools(messages: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Resolve the tools available after applying every transcript delta in order."""

    tools: dict[str, dict[str, Any]] = {}
    for message in messages:
        if message.get("role") != "system":
            continue
        for reference in cast(Sequence[Mapping[str, Any]], message.get("toolsRemoved") or ()):
            tools.pop(cast(str, reference["name"]), None)
        for tool in cast(Sequence[Mapping[str, Any]], message.get("toolsAdded") or ()):
            tools[cast(str, tool["name"])] = dict(tool)
    return list(tools.values())


def current_system_message(messages: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    """Replay every system message into one leading message holding the prompt and tools."""

    contents: list[str] = []
    sections: dict[str, str] = {}
    timestamp: int | None = None
    found = False
    for message in messages:
        if message.get("role") != "system":
            continue
        found = True
        if timestamp is None:
            timestamp = cast(int, message.get("timestamp") or 0)
        text = content_text(message.get("content"))
        if text:
            contents.append(text)
        for name, value in cast(Mapping[str, str | None], message.get("sections") or {}).items():
            if value is None:
                sections.pop(name, None)
            else:
                sections[name] = value
    tools = current_tools(messages)
    if not found:
        return None
    head: dict[str, Any] = {
        "role": "system",
        "content": "\n\n".join(contents),
        "timestamp": 0 if timestamp is None else timestamp,
    }
    if sections:
        head["sections"] = sections
    if tools:
        head["toolsAdded"] = tools
    return head


def collapse_system_messages(
    messages: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Return the replayed leading system message and the conversation messages without system entries."""

    head = current_system_message(messages)
    rest = [dict(message) for message in messages if message.get("role") != "system"]
    return head, rest


def transform_messages(
    messages: Sequence[Mapping[str, Any]],
    model: Model,
    *,
    api: str,
    normalize_tool_call_id: Callable[[str], str] | None = None,
) -> list[dict[str, Any]]:
    """Adapt a transcript to ``model``: image downgrade, cross-model thinking, and tool-call IDs.

    Thinking signed by another model is dropped because signatures only replay for their producer.
    A tool-call ID another provider minted is normalized when the adapter supplies a rule, and the
    matching tool result is rewritten too. Failed and aborted assistant messages are skipped.
    """

    tool_call_ids: dict[str, str] = {}
    transformed: list[dict[str, Any]] = []
    for message in messages:
        role = message.get("role")
        if role == "system":
            transformed.append(dict(message))
            continue
        if role == "user":
            transformed.append(_downgrade_images(dict(message), model, NON_VISION_USER_IMAGE_PLACEHOLDER))
            continue
        if role == "toolResult":
            result = _downgrade_images(dict(message), model, NON_VISION_TOOL_IMAGE_PLACEHOLDER)
            normalized = tool_call_ids.get(cast(str, message.get("toolCallId")))
            if normalized is not None:
                result["toolCallId"] = normalized
            transformed.append(result)
            continue
        if role != "assistant":
            transformed.append(dict(message))
            continue
        if message.get("stopReason") in ("error", "aborted"):
            continue
        same_model = (
            message.get("provider") == model.provider
            and message.get("api") == api
            and message.get("model") == model.model_id
        )
        content: list[dict[str, Any]] = []
        for block in cast(Sequence[Mapping[str, Any]], message.get("content") or ()):
            kind = block.get("type")
            if kind == "thinking":
                if not same_model:
                    continue
                if (
                    not block.get("redacted")
                    and not block.get("thinkingSignature")
                    and not block.get("thinking")
                ):
                    continue
                content.append(dict(block))
            elif kind == "text":
                content.append(dict(block))
            elif kind == "toolCall":
                call = dict(block)
                if not same_model:
                    call.pop("thoughtSignature", None)
                    if normalize_tool_call_id is not None:
                        original = cast(str, call.get("id") or "")
                        normalized = normalize_tool_call_id(original)
                        if normalized != original:
                            tool_call_ids[original] = normalized
                            call["id"] = normalized
                content.append(call)
            else:
                content.append(dict(block))
        assistant = dict(message)
        assistant["content"] = content
        transformed.append(assistant)
    return transformed


def _downgrade_images(
    message: dict[str, Any], model: Model, placeholder: str
) -> dict[str, Any]:
    if "image" in model.input:
        return message
    content = message.get("content")
    if not isinstance(content, (list, tuple)):
        return message
    replaced: list[dict[str, Any]] = []
    previous_was_placeholder = False
    for block in cast(Sequence[Mapping[str, Any]], content):
        if block.get("type") != "image":
            replaced.append(dict(block))
            previous_was_placeholder = block.get("type") == "text" and block.get("text") == placeholder
            continue
        if not previous_was_placeholder:
            replaced.append({"type": "text", "text": placeholder})
        previous_was_placeholder = True
    message["content"] = replaced
    return message


def has_tool_history(messages: Sequence[Mapping[str, Any]]) -> bool:
    """Whether the transcript carries any tool call or tool result."""

    for message in messages:
        if message.get("role") == "toolResult":
            return True
        if message.get("role") == "assistant" and any(
            block.get("type") == "toolCall"
            for block in cast(Sequence[Mapping[str, Any]], message.get("content") or ())
        ):
            return True
    return False


__all__ = [
    "NON_VISION_TOOL_IMAGE_PLACEHOLDER",
    "NON_VISION_USER_IMAGE_PLACEHOLDER",
    "collapse_system_messages",
    "content_text",
    "current_system_message",
    "current_tools",
    "has_tool_history",
    "system_message_text",
    "transform_messages",
]
