"""Numeric identities and commit sequences.

Brands are compile-time only, exactly as in v1.3.0: IDs and sequences remain ordinary Python
integers in memory, JSON, JSONL, and SQLite. ``NewType`` gives each record kind a distinct static
type without changing the runtime value. Apply a brand only at a trusted allocation or decoding
boundary through :func:`id_from_number` or :func:`seq_from_number`.
"""

from __future__ import annotations

from typing import NewType, TypeVar, cast

Id = NewType("Id", int)
"""An erased nominal integer identifying one durable record kind."""

ConversationId = NewType("ConversationId", int)
EntryId = NewType("EntryId", int)
TaskId = NewType("TaskId", int)
SubmissionId = NewType("SubmissionId", int)
DocumentId = NewType("DocumentId", int)

Seq = NewType("Seq", int)
"""Strictly increasing sequence assigned to one atomic storage commit; gaps are permitted."""

ROOT_CONVERSATION_ID: ConversationId = ConversationId(1)
"""The root conversation always uses this reserved ID."""

IdT = TypeVar("IdT", bound=int)


def require_positive_int(value: object, label: str) -> int:
    """Validate an integer at a trusted numeric boundary and return it unchanged."""

    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise TypeError(f"{value!r} is not a positive {label}")
    return value


def id_from_number(value: int) -> IdT:  # type: ignore[type-var]
    """Apply an erased ID brand at a trusted numeric allocation or decoding boundary."""

    return cast(IdT, require_positive_int(value, "ID"))


def seq_from_number(value: int) -> Seq:
    """Apply the erased commit-sequence brand at a trusted storage boundary."""

    return Seq(require_positive_int(value, "sequence"))


__all__ = [
    "ROOT_CONVERSATION_ID",
    "ConversationId",
    "DocumentId",
    "EntryId",
    "Id",
    "Seq",
    "SubmissionId",
    "TaskId",
    "id_from_number",
    "require_positive_int",
    "seq_from_number",
]
