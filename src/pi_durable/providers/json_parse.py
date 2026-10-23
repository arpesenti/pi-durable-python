"""Tool-call argument parsing shared by the model adapters.

Port of pi-ai 1.3.0 ``utils/json-parse.ts``. Streaming providers send a call's arguments as JSON
text split across deltas, and some models emit raw control characters inside JSON strings. The
repair pass escapes those, and a finished call whose text is still not a valid JSON object reports
``argumentsError`` instead of pretending the call was empty.
"""

from __future__ import annotations

import json
from typing import Any

_VALID_ESCAPES = frozenset('"\\/bfnrt')
_CONTROL_ESCAPES = {
    "\b": "\\b",
    "\f": "\\f",
    "\n": "\\n",
    "\r": "\\r",
    "\t": "\\t",
}


def repair_json(text: str) -> str:
    """Escape raw control characters and double backslashes before invalid escapes, inside strings."""

    repaired: list[str] = []
    in_string = False
    index = 0
    while index < len(text):
        char = text[index]
        if not in_string:
            repaired.append(char)
            if char == '"':
                in_string = True
            index += 1
            continue
        if char == '"':
            repaired.append(char)
            in_string = False
            index += 1
            continue
        if char == "\\":
            following = text[index + 1] if index + 1 < len(text) else None
            if following is None:
                repaired.append("\\\\")
                index += 1
                continue
            if following == "u":
                digits = text[index + 2 : index + 6]
                if len(digits) == 4 and all(digit in "0123456789abcdefABCDEF" for digit in digits):
                    repaired.append(f"\\u{digits}")
                    index += 6
                    continue
            if following in _VALID_ESCAPES:
                repaired.append(f"\\{following}")
                index += 2
                continue
            repaired.append("\\\\")
            index += 1
            continue
        if ord(char) <= 0x1F:
            repaired.append(_CONTROL_ESCAPES.get(char, f"\\u{ord(char):04x}"))
        else:
            repaired.append(char)
        index += 1
    return "".join(repaired)


def parse_json_with_repair(text: str) -> Any:
    """Parse ``text``, retrying once with :func:`repair_json`."""

    try:
        return json.loads(text)
    except ValueError:
        repaired = repair_json(text)
        if repaired == text:
            raise
        return json.loads(repaired)


def parse_tool_call_arguments(text: str | None) -> tuple[dict[str, Any], str | None]:
    """Parse a finished call's argument text into an object, or report why it is not one."""

    if text is None or not text.strip():
        return {}, None
    try:
        parsed = parse_json_with_repair(text)
        if not isinstance(parsed, dict):
            raise ValueError("Tool arguments must be a JSON object")
        return parsed, None
    except ValueError as error:
        return {}, f"{error}\n\nReceived arguments:\n{text}"


def parse_streaming_json(text: str | None) -> dict[str, Any]:
    """Parse a still-growing argument text, returning ``{}`` until it is valid JSON.

    The port does not carry pi-ai's partial-JSON dependency; a partial call therefore shows no
    arguments until its text parses, and the finished call is the authoritative value.
    """

    if text is None or not text.strip():
        return {}
    try:
        parsed = parse_json_with_repair(text)
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


__all__ = [
    "parse_json_with_repair",
    "parse_streaming_json",
    "parse_tool_call_arguments",
    "repair_json",
]
