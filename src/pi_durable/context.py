"""Model-context derivation from committed transcript entries.

The derivation follows the reference rules: the newest head marker fixes the range; the
newest edit in the range wins per target; tool results move directly after their assistant
in call order; missing results are synthesized; failed/aborted/deferred assistant messages
are excluded from future provider requests; and the baseline system message leads when it
must.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from .json import copy_json
from .storage.base import Storage

SCAN_PAGE_SIZE = 256
EXCLUDED_STOP_REASONS = {"aborted", "error", "deferred"}
MISSING_RESULT_TEXT = "Tool result unavailable: history ends before this call completed."
SYSTEM_KIND = "pi.system"


def system_message(
    sections: Mapping[str, str | None] | None = None,
    *,
    tools_added: Sequence[Mapping[str, Any]] | None = None,
    tools_removed: Sequence[Any] | None = None,
    timestamp: int = 0,
) -> dict[str, Any]:
    message: dict[str, Any] = {"role": "system", "content": "", "timestamp": timestamp}
    if sections is not None:
        message["sections"] = dict(sections)
    if tools_added:
        message["toolsAdded"] = [copy_json(tool) for tool in tools_added]
    if tools_removed:
        message["toolsRemoved"] = copy_json(list(tools_removed))
    return message


def user_message(content: Any, *, timestamp: int = 0) -> dict[str, Any]:
    return {"role": "user", "content": copy_json(content), "timestamp": timestamp}


def assistant_message(
    content: Sequence[Mapping[str, Any]],
    *,
    stop_reason: str = "stop",
    timestamp: int = 0,
    **extra: Any,
) -> dict[str, Any]:
    message = {
        "role": "assistant",
        "content": copy_json(list(content)),
        "stopReason": stop_reason,
        "timestamp": timestamp,
    }
    message.update(copy_json(extra))
    return message


def text_content(text: str) -> dict[str, Any]:
    return {"type": "text", "text": text}


def tool_call(call_id: str, name: str, arguments: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {"type": "toolCall", "id": call_id, "name": name, "arguments": copy_json(dict(arguments or {}))}


def tool_result_message(
    call_id: str,
    name: str,
    content: Any,
    *,
    is_error: bool = False,
    details: Any = None,
    timestamp: int = 0,
) -> dict[str, Any]:
    message: dict[str, Any] = {
        "role": "toolResult",
        "toolCallId": call_id,
        "toolName": name,
        "content": copy_json(content),
        "isError": is_error,
        "timestamp": timestamp,
    }
    if details is not None:
        message["details"] = copy_json(details)
    return message


def message_role(message: Mapping[str, Any]) -> str:
    role = message.get("role")
    if role == "tool_result":
        return "toolResult"
    return str(role or "")


def is_system(message: Mapping[str, Any]) -> bool:
    return message_role(message) == "system"


def is_tool_result(message: Mapping[str, Any]) -> bool:
    return message_role(message) == "toolResult"


def is_assistant(message: Mapping[str, Any]) -> bool:
    return message_role(message) == "assistant"


def is_user(message: Mapping[str, Any]) -> bool:
    return message_role(message) == "user"


async def capture_context_bounds(
    storage: Storage, conversation_id: int, at: int | None = None
) -> dict[str, Any] | None:
    """Capture the head marker and tail that fix one committed context range."""
    if at is None:
        page = await storage.scan_entries({"conversationId": conversation_id}, 1, None)
        tail = page["items"][0]["id"] if page["items"] else None
        if tail is None:
            return None
    else:
        if await storage.entry(conversation_id, at) is None:
            raise ValueError(f"Entry {at} is not visible from conversation {conversation_id}")
        tail = at
    head = await storage.find_latest_head_marker(conversation_id, tail)
    return {"head": head, "tail": tail}


async def read_context(
    session: Any, conversation_id: int, at: int | None = None
) -> dict[str, Any]:
    """Committed context of one conversation: bounds on the line, derivation off it."""
    bounds = await session.read_on_line(lambda: capture_context_bounds(session.storage, conversation_id, at))
    return await derive_context(session.storage, conversation_id, bounds)


async def derive_context(
    storage: Storage, conversation_id: int, bounds: Mapping[str, Any] | None
) -> dict[str, Any]:
    """Derive the active transcript and model context within captured bounds."""
    if bounds is None:
        return {"head": None, "entries": [], "contributions": [], "messages": []}
    entries = await _scan_range(storage, conversation_id, bounds)
    return derive_range(bounds, entries)


def derive_range(bounds: Mapping[str, Any], entries: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    edits: dict[int, Mapping[str, Any]] = {}
    for entry in entries:
        for edit in entry.get("edits") or []:
            edits[edit["target"]] = edit
    active = select_active(bounds.get("head"), entries)
    contributions = [contribute(entry, edits.get(entry["id"])) for entry in active]
    settled, open_messages = settle([], [message for group in contributions for message in group])
    baseline = find_baseline(bounds.get("head"), active)
    messages = lead_with_system([*settled, *order_tool_results(open_messages)], baseline)
    return {
        "head": copy_json(bounds.get("head")),
        "entries": copy_json(list(active)),
        "contributions": copy_json(contributions),
        "messages": copy_json(messages),
    }


def select_active(head: Mapping[str, Any] | None, range_entries: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    if head is None:
        return list(range_entries)
    return [head, *[entry for entry in range_entries if entry.get("head") is None]]


def contribute(entry: Mapping[str, Any], edit: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if edit is not None and edit.get("action") == "omit":
        return []
    contributed = edit.get("messages") if edit is not None and edit.get("action") == "replace" else entry.get("model")
    messages = contributed or []
    return [
        message
        for message in messages
        if not (is_assistant(message) and message.get("stopReason") in EXCLUDED_STOP_REASONS)
    ]


def find_baseline(head: Mapping[str, Any] | None, entries: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    """The baseline's system message, ``None`` when the range cannot have one, or missing while later entries may."""
    for entry in entries:
        if head is not None and entry["id"] <= head["id"]:
            continue
        if entry.get("kind") == SYSTEM_KIND:
            for message in entry.get("model") or []:
                if is_system(message):
                    return message
            return None
        if head is None and _carries_system_message(entry):
            return None
    return None


def _carries_system_message(entry: Mapping[str, Any]) -> bool:
    if any(is_system(message) for message in entry.get("model") or []):
        return True
    for edit in entry.get("edits") or []:
        if edit.get("action") == "replace" and any(is_system(message) for message in edit.get("messages") or []):
            return True
    return False


def lead_with_system(messages: list[dict[str, Any]], baseline: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    index = next((i for i, message in enumerate(messages) if is_system(message)), -1)
    if index <= 0:
        return messages
    system = messages[index]
    if system is not baseline and any(not is_user(message) for message in messages[:index]):
        return messages
    return [system, *messages[:index], *messages[index + 1 :]]


def settle(
    settled: Sequence[Any], open_messages: Sequence[Any]
) -> tuple[list[Any], list[Any]]:
    """Move ordered messages before the last assistant message of ``open`` to ``settled``."""
    last = -1
    for index, message in enumerate(open_messages):
        if is_assistant(message):
            last = index
    if last <= 0:
        return list(settled), list(open_messages)
    return [*settled, *order_tool_results(open_messages[:last])], list(open_messages[last:])


def order_tool_results(messages: Iterable[Mapping[str, Any]]) -> list[Any]:
    """Place each assistant's tool results directly after it in call order.

    A missing result is synthesized; unmatched results are dropped.
    """
    source = list(messages)
    ordered: list[Any] = []
    for index, message in enumerate(source):
        if is_tool_result(message):
            continue
        ordered.append(message)
        if not is_assistant(message):
            continue
        calls = [item for item in message.get("content") or [] if item.get("type") == "toolCall"]
        if not calls:
            continue
        results: dict[str, tuple[int, Mapping[str, Any]]] = {}
        for next_index in range(index + 1, len(source)):
            candidate = source[next_index]
            if is_assistant(candidate):
                break
            if is_tool_result(candidate) and candidate.get("toolCallId") not in results:
                results[candidate["toolCallId"]] = (next_index, candidate)
        for call in calls:
            found = results.get(call["id"])
            if found is None:
                ordered.append(_missing_result(call, int(message.get("timestamp") or 0)))
            else:
                ordered.append(found[1])
    return ordered


def _missing_result(call: Mapping[str, Any], timestamp: int) -> dict[str, Any]:
    return {
        "role": "toolResult",
        "toolCallId": call["id"],
        "toolName": call.get("name"),
        "content": [text_content(MISSING_RESULT_TEXT)],
        "isError": True,
        "details": {"reason": "missing_result"},
        "timestamp": timestamp,
    }


async def _scan_range(
    storage: Storage, conversation_id: int, bounds: Mapping[str, Any]
) -> list[Mapping[str, Any]]:
    head = bounds.get("head")
    query: dict[str, Any] = {"conversationId": conversation_id, "maxEntryId": bounds["tail"]}
    if head is not None:
        query["minEntryId"] = head["head"]
    entries: list[Mapping[str, Any]] = []
    cursor = None
    while True:
        page = await storage.scan_entries(query, SCAN_PAGE_SIZE, cursor)
        entries.extend(page["items"])
        cursor = page.get("next")
        if cursor is None:
            break
    entries.reverse()
    return entries


__all__ = [
    "EXCLUDED_STOP_REASONS",
    "MISSING_RESULT_TEXT",
    "assistant_message",
    "capture_context_bounds",
    "contribute",
    "derive_context",
    "derive_range",
    "find_baseline",
    "lead_with_system",
    "message_role",
    "order_tool_results",
    "read_context",
    "select_active",
    "settle",
    "system_message",
    "text_content",
    "tool_call",
    "tool_result_message",
    "user_message",
]
