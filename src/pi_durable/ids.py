"""Numeric ID and commit-sequence helpers.

IDs and sequences are ordinary Python integers in memory, JSON, JSONL and SQLite. The
distinct Python aliases below document intent; they carry no runtime behavior.
"""

from __future__ import annotations

ConversationId = int
EntryId = int
SubmissionId = int
DocumentId = int
TaskId = int
Seq = int

ROOT_CONVERSATION_ID: ConversationId = 1


def id_from_number(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{value!r} is not an ID")
    return value


__all__ = [
    "ROOT_CONVERSATION_ID",
    "ConversationId",
    "DocumentId",
    "EntryId",
    "Seq",
    "SubmissionId",
    "TaskId",
    "id_from_number",
]
