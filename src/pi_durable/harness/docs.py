"""Built-in conversation documents: agent, provider, live run, inbox, and usage."""

from __future__ import annotations

import secrets
import time
from collections.abc import Mapping, MutableMapping
from typing import Any

from ..documents import define_doc
from ..json import copy_json

ALWAYS_CHECKPOINT = lambda value, ops, info: True
NEVER_CHECKPOINT = lambda value, ops, info: False


def uuid7() -> str:
    """A provider-facing UUIDv7 (time-ordered) without a third-party dependency."""
    milliseconds = int(time.time() * 1000)
    random_bits = secrets.randbits(74)
    value = (milliseconds << 80) | (0x7 << 76) | ((random_bits >> 62) & 0xFFF) << 64
    value |= 0b10 << 62
    value |= random_bits & ((1 << 62) - 1)
    text = f"{value:032x}"
    return f"{text[0:8]}-{text[8:12]}-{text[12:16]}-{text[16:20]}-{text[20:32]}"


AgentDoc = define_doc(
    kind="pi.agent",
    version=1,
    scope="conversation",
    history="rewindable",
    fork="asOf",
    initial=dict,
    checkpoint_when=ALWAYS_CHECKPOINT,
)

ProviderDoc = define_doc(
    kind="pi.provider",
    version=1,
    scope="conversation",
    history="latest",
    fork="initial",
    initial=lambda: {"sessionId": uuid7()},
    checkpoint_when=ALWAYS_CHECKPOINT,
)

LiveDoc = define_doc(
    kind="pi.live",
    version=1,
    scope="conversation",
    history="latest",
    fork="initial",
    initial=dict,
    checkpoint_when=ALWAYS_CHECKPOINT,
)

InboxDoc = define_doc(
    kind="pi.inbox",
    version=1,
    scope="conversation",
    history="latest",
    fork="initial",
    initial=lambda: {"items": []},
    checkpoint_when=ALWAYS_CHECKPOINT,
)

UsageDoc = define_doc(
    kind="pi.usage",
    version=1,
    scope="conversation",
    history="latest",
    fork="initial",
    initial=lambda: {"models": {}, "tools": {}},
    checkpoint_when=ALWAYS_CHECKPOINT,
)


def empty_usage() -> dict[str, Any]:
    return {"input": 0, "output": 0, "total": 0, "cost": 0.0}


def add_usage(target: MutableMapping[str, Any], usage: Mapping[str, Any] | None) -> None:
    if not usage:
        return
    for name in ("input", "output", "total", "cost"):
        value = usage.get(name)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            target[name] = target.get(name, 0) + value


def tool_declaration(tool: Any) -> dict[str, Any]:
    return tool.declaration if hasattr(tool, "declaration") else copy_json(tool)


def normalize_tool_reference(reference: Any) -> str | None:
    if isinstance(reference, str):
        return reference
    if isinstance(reference, Mapping):
        name = reference.get("name")
        return name if isinstance(name, str) else None
    return None


__all__ = [
    "ALWAYS_CHECKPOINT",
    "NEVER_CHECKPOINT",
    "AgentDoc",
    "InboxDoc",
    "LiveDoc",
    "ProviderDoc",
    "UsageDoc",
    "add_usage",
    "empty_usage",
    "normalize_tool_reference",
    "tool_declaration",
    "uuid7",
]
