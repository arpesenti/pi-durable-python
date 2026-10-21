"""Immutable SQLite identifier history, translated from v1.3.0 ``sqlite/schema.ts``.

Every migration and runtime query renders its object names through this module. Existing values are
never changed or removed: a rename adds an entry so historical migrations keep rendering their
original object names.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

TABLE_NAMES: Mapping[str, str] = MappingProxyType(
    {
        "durableSchema": "durable_schema",
        "durableMetadata": "durable_metadata",
        "recordIds": "record_ids",
        "conversations": "conversations",
        "entries": "entries",
        "tasks": "tasks",
        "submissions": "submissions",
        "documents": "documents",
        "documentRevisions": "document_revisions",
    }
)

INDEX_NAMES: Mapping[str, str] = MappingProxyType(
    {
        "conversationsByOwnerConversation": "conversations_by_owner_conversation",
        "conversationsByOwnerTask": "conversations_by_owner_task",
        "entriesByConversation": "entries_by_conversation",
        "entryHeadsByConversation": "entry_heads_by_conversation",
        "tasksByStatus": "tasks_by_status",
        "tasksByConversation": "tasks_by_conversation",
        "tasksByKind": "tasks_by_kind",
        "tasksByAbortRequested": "tasks_by_abort_requested",
        "tasksByBackground": "tasks_by_background",
        "submissionsByRequest": "submissions_by_request",
        "submissionsByConversation": "submissions_by_conversation",
        "submissionsByStatus": "submissions_by_status",
        "documentsByAddress": "documents_by_address",
        "documentsByScope": "documents_by_scope",
        "documentsByScopeKind": "documents_by_scope_kind",
        "documentRevisionsByKind": "document_revisions_by_kind",
    }
)

_PREFIX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class SqliteSchema:
    """The table and index identifiers used by every migration and storage query."""

    tables: Mapping[str, str]
    indexes: Mapping[str, str]


def validate_sqlite_schema_prefix(prefix: str) -> None:
    """Validate a prefix before it becomes part of a SQLite identifier."""

    if not _PREFIX.fullmatch(prefix):
        raise ValueError(f"Invalid SQLite schema prefix {json.dumps(prefix)}")
    if prefix.lower().startswith("sqlite_"):
        raise ValueError(f"SQLite schema prefix {json.dumps(prefix)} is reserved")


def _prefix_identifiers(names: Mapping[str, str], prefix: str) -> Mapping[str, str]:
    return MappingProxyType({key: f"{prefix}{name}" for key, name in names.items()})


def create_sqlite_schema(prefix: str | None = None) -> SqliteSchema:
    """Build the identifiers used by every migration and storage query."""

    if prefix is not None:
        validate_sqlite_schema_prefix(prefix)
    actual = prefix or ""
    return SqliteSchema(
        tables=_prefix_identifiers(TABLE_NAMES, actual),
        indexes=_prefix_identifiers(INDEX_NAMES, actual),
    )


DEFAULT_SQLITE_SCHEMA = create_sqlite_schema()


__all__ = [
    "DEFAULT_SQLITE_SCHEMA",
    "INDEX_NAMES",
    "TABLE_NAMES",
    "SqliteSchema",
    "create_sqlite_schema",
    "validate_sqlite_schema_prefix",
]
