"""Typed entry kinds.

An ``Entry`` token carries the persisted discriminator and narrows an entry record by ``kind``.
The built-in tokens name the transcript entries durable itself writes; applications define their
own with :func:`define_entry`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Generic, Never, TypeGuard, TypeVar, cast

from .json import JsonValue
from .types import TypedEntry

D = TypeVar("D", bound=JsonValue)


@dataclass(frozen=True, slots=True)
class Entry(Generic[D]):
    """A typed entry kind with a narrowing guard for its ``data`` payload."""

    kind: str

    def is_(self, entry: Mapping[str, Any] | None) -> TypeGuard[TypedEntry[D]]:
        """Return whether ``entry`` is present and has this token's kind."""

        return entry is not None and entry.get("kind") == self.kind


def define_entry(kind: str) -> Entry[D]:
    """Define a typed entry kind whose guard narrows by ``EntryRecord.kind``."""

    if not isinstance(kind, str) or not kind:
        raise TypeError("Entry kind must be a non-empty string")
    return cast(Entry[D], Entry(kind))


#: User input: ``model`` is ``[UserMessage]``. Written by submissions.
UserEntry: Entry[Never] = define_entry("pi.user")
#: Provider result with any stop reason: ``model`` is ``[AssistantMessage]``. Written by generation.
AssistantEntry: Entry[Never] = define_entry("pi.assistant")
#: Positional prompt and tool change: ``model`` is ``[SystemMessage]`` with empty ``content``.
SystemEntry: Entry[Never] = define_entry("pi.system")
#: Tool result: ``model`` is ``[ToolResultMessage]``, whose content ends with the rendered
#: diagnostics block; ``data`` holds the structured diagnostics, possibly none.
ToolResultEntry: Entry[dict[str, JsonValue]] = define_entry("pi.tool-result")
#: Start of a new context: always ``head: "self"``, with ``model`` absent for a plain reset or
#: ``[UserMessage]`` carrying the handoff text.
ResetEntry: Entry[Never] = define_entry("pi.reset")
#: Compaction summary: ``model`` is ``[UserMessage]`` with the wrapped summary, ``head`` the first
#: kept entry.
CompactionEntry: Entry[dict[str, JsonValue]] = define_entry("pi.compaction")


__all__ = [
    "AssistantEntry",
    "CompactionEntry",
    "Entry",
    "ResetEntry",
    "SystemEntry",
    "ToolResultEntry",
    "UserEntry",
    "define_entry",
]
