"""Public Harness types.

The Harness layer grows in later tickets; this module already holds the value shapes the Session
produces for it.
"""

from __future__ import annotations

from typing import TypedDict

from ..types import EntryRecord, EntryWithHead, MessageRecord


class ContextView(TypedDict):
    """One conversation's committed transcript and the model context derived from it.

    ``head`` is the newest visible head marker, if any. ``entries`` is the raw active transcript,
    ``contributions`` holds each of their model messages after edits and excluded stop reasons, and
    ``messages`` is the provider request context: tool results ordered after their call, missing
    ones synthesized, and the baseline system message first.
    """

    head: EntryWithHead | None
    entries: list[EntryRecord]
    contributions: list[list[MessageRecord]]
    messages: list[MessageRecord]


__all__ = ["ContextView"]
