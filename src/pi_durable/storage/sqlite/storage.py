"""Portable SQLite implementation of the storage contract, translated from v1.3.0.

One SQL transaction is one Session commit. Table records stay in their record tables as JSON, while
indexed identities are duplicated into columns so scans, ancestry walks, and document history use
indexes instead of decoding every row.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, NotRequired, TypedDict, TypeVar, cast, overload

from ...context import Context
from ...delta import apply
from ...errors import StorageRequestError
from ...ids import (
    ConversationId,
    DocumentId,
    EntryId,
    Seq,
    SubmissionId,
    TaskId,
    id_from_number,
    seq_from_number,
)
from ...json import JsonObject
from ...types import (
    ConversationQuery,
    ConversationRecord,
    Cursor,
    DocumentAddress,
    DocumentContent,
    DocumentCreate,
    DocumentPoint,
    DocumentQuery,
    DocumentRecord,
    EntryLookup,
    EntryQuery,
    EntryRecord,
    EntryWithHead,
    Page,
    ScanOrder,
    StorageWrite,
    StoredDocument,
    SubmissionQuery,
    SubmissionRecord,
    TaskQuery,
    TaskRecord,
)
from ..scan import ScanStart, next_cursor, scan_start
from .database import SqliteDatabase, SqliteExecutor, SqliteValue
from .migrations import apply_sqlite_migrations_for_schema
from .schema import SqliteSchema, create_sqlite_schema

T = TypeVar("T")

_MAX_SAFE_INTEGER = 9_007_199_254_740_991


class SqliteStorageOptions(TypedDict):
    """Options for opening SQLite storage.

    ``prefix`` namespaces every durable table and index. Existing data is not migrated when it
    changes. Connection tuning belongs to the host adapter, not the portable core.
    """

    prefix: NotRequired[str]


@dataclass(frozen=True, slots=True)
class _ScopeColumns:
    scope_kind: str
    owner_id: int


@dataclass(frozen=True, slots=True)
class _AddressParts:
    kind: str
    scope_kind: str
    owner_id: int
    family: int
    key_value: str


@dataclass(slots=True)
class _DocumentAction:
    create: DocumentCreate | None = None
    copy: dict[str, Any] | None = None
    content: DocumentContent | None = None
    retire: bool = False


def _is_safe_int(value: object) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and abs(value) <= _MAX_SAFE_INTEGER
    )


def _parse_json(value: str) -> Any:
    return json.loads(value)


def _encode_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), allow_nan=False)


def _encode_indexed_string(value: str) -> str:
    """stdlib ``sqlite3`` cannot bind lone surrogates; JSON encoding keeps identities lossless."""

    return json.dumps(value)


def _cursor_id(cursor: Cursor | None) -> DocumentId | None:
    if cursor is None:
        return None
    after = cursor.get("after")
    if not _is_safe_int(after):
        raise StorageRequestError("Invalid storage cursor")
    return id_from_number(cast(int, after))


def _page(
    values: Sequence[Any],
    limit: int,
    order: ScanOrder = "ascending",
) -> Page[Any, Cursor]:
    items = list(values[:limit])
    if len(values) <= limit:
        return {"items": items}
    return {"items": items, "next": next_cursor(items[-1]["id"], order)}


def _scan_sql(start: ScanStart) -> tuple[str, int, str]:
    if start.order == "ascending":
        return "id > ?", -1 if start.after is None else start.after, "ASC"
    return "id < ?", _MAX_SAFE_INTEGER if start.after is None else start.after, "DESC"


def _scope_columns(scope: Any) -> _ScopeColumns:
    kind = scope["kind"]
    if kind == "session":
        return _ScopeColumns("session", 0)
    if kind == "conversation":
        return _ScopeColumns("conversation", scope["conversationId"])
    return _ScopeColumns("task", scope["taskId"])


def _address_parts(address: Any) -> _AddressParts:
    scope = _scope_columns(address["scope"])
    key = address.get("key")
    return _AddressParts(
        kind=_encode_indexed_string(address["kind"]),
        scope_kind=scope.scope_kind,
        owner_id=scope.owner_id,
        family=0 if key is None else 1,
        key_value=_encode_indexed_string("" if key is None else key),
    )


def _address_key(address: Any) -> str:
    parts = _address_parts(address)
    return json.dumps(
        [parts.kind, parts.scope_kind, parts.owner_id, parts.family, parts.key_value],
        separators=(",", ":"),
    )


def _is_alive_at(record: DocumentRecord, at: DocumentPoint) -> bool:
    if at == "current":
        return "retiredAt" not in record
    return record["createdAt"] <= at and (
        "retiredAt" not in record or at < record["retiredAt"]
    )


def _is_current_only(record: Any) -> bool:
    return record["scope"]["kind"] != "conversation" or record.get("history") == "latest"


def _write_id(write: StorageWrite) -> int | None:
    item = cast(dict[str, Any], write)
    kind = item["type"]
    if kind in ("conversation", "entry", "task", "submission"):
        return cast(int, item["value"]["id"])
    if kind in ("document.create", "document.copy"):
        return cast(int, item["record"]["id"])
    return None


class SqliteStorage:
    """Portable SQLite implementation of the storage contract."""

    def __init__(
        self,
        database: SqliteDatabase,
        schema: SqliteSchema,
        next_id: int,
    ) -> None:
        self._db = database
        self._schema = schema
        self._next_id = next_id
        self._closed = False
        self._closing: asyncio.Future[None] | None = None
        self._admitted_reads = 0
        self._reads_drained: asyncio.Future[None] | None = None

    @staticmethod
    async def open(
        database: SqliteDatabase,
        options: SqliteStorageOptions | None = None,
    ) -> SqliteStorage:
        """Initialize storage over an owned SQLite database facade."""

        prefix = None if options is None else options.get("prefix")
        try:
            schema = create_sqlite_schema(prefix)
            await apply_sqlite_migrations_for_schema(database, schema)
            metadata = await database.get(
                f"SELECT next_id, next_seq FROM {schema.tables['durableMetadata']} "
                "WHERE singleton = 1"
            )
            if metadata is None:
                raise RuntimeError("Durable SQLite metadata is missing")
            return SqliteStorage(database, schema, int(metadata["next_id"]))
        except BaseException:
            try:
                await database.close()
            except Exception:
                pass
            raise

    # ── Commits ─────────────────────────────────────────────────────────────

    async def commit(self, writes: Sequence[StorageWrite], context: Context) -> Seq:
        self._assert_open()
        document_actions = self._prepare_document_actions(writes)
        candidate_next_id = self._candidate_next_id(writes)
        metadata_table = self._schema.tables["durableMetadata"]

        async def run_transaction(transaction: SqliteExecutor) -> Seq:
            metadata = await transaction.get(
                f"SELECT next_id, next_seq FROM {metadata_table} WHERE singleton = 1"
            )
            if metadata is None:
                raise RuntimeError("Durable SQLite metadata is missing")
            committed_seq = seq_from_number(metadata["next_seq"])
            await self._check_global_ids(transaction, writes)
            await self._check_document_actions(transaction, document_actions)
            for write in writes:
                await self._apply_table_write(transaction, write, committed_seq)
            await self._apply_document_actions(transaction, document_actions, committed_seq)
            await transaction.run(
                f"UPDATE {metadata_table} SET next_id = ?, next_seq = ? WHERE singleton = 1",
                str(max(int(metadata["next_id"]), candidate_next_id)),
                committed_seq + 1,
            )
            return committed_seq

        seq = await self._db.transaction(run_transaction)
        self._next_id = max(self._next_id, candidate_next_id)
        return seq

    async def mint_id(self) -> int:
        self._assert_open()
        if not _is_safe_int(self._next_id):
            raise RuntimeError("ID space is exhausted")
        value = self._next_id
        self._next_id += 1
        return value

    # ── Table reads ─────────────────────────────────────────────────────────

    async def conversation(self, id: ConversationId, context: Context) -> ConversationRecord | None:
        self._assert_open()
        row = await self._db.get(
            f"SELECT record FROM {self._schema.tables['conversations']} WHERE id = ?", id
        )
        return None if row is None else cast(ConversationRecord, _parse_json(row["record"]))

    async def scan_conversations(
        self,
        query: ConversationQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[ConversationRecord, Cursor]:
        self._assert_open()
        start = scan_start(query.get("order"), cursor, "ascending")
        clause, param, direction = _scan_sql(start)
        clauses = [clause]
        params: list[SqliteValue] = [param]
        if "ownerConversationId" in query:
            clauses.append("owner_conversation_id = ?")
            params.append(query["ownerConversationId"])
        if "ownerTaskId" in query:
            clauses.append("owner_task_id = ?")
            params.append(query["ownerTaskId"])
        params.append(limit + 1)
        rows = await self._db.all(
            f"SELECT record FROM {self._schema.tables['conversations']} "
            f"WHERE {' AND '.join(clauses)} ORDER BY id {direction} LIMIT ?",
            *params,
        )
        return _page(
            [_parse_json(row["record"]) for row in rows], limit, start.order
        )

    @overload
    async def entry(self, id: EntryId, context: Context, /) -> EntryLookup | None: ...

    @overload
    async def entry(
        self,
        conversation_id: ConversationId,
        id: EntryId,
        context: Context,
        /,
    ) -> EntryLookup | None: ...

    async def entry(
        self,
        id_or_conversation_id: EntryId | ConversationId,
        id_or_context: EntryId | Context | None = None,
        context: Context | None = None,
    ) -> EntryLookup | None:
        return await self._admit_read(
            lambda: self._read_entry(id_or_conversation_id, id_or_context, context)
        )

    async def _read_entry(
        self,
        id_or_conversation_id: EntryId | ConversationId,
        id_or_context: EntryId | Context | None,
        context: Context | None,
    ) -> EntryLookup | None:
        # Public call shapes are ``entry(id, context)`` and ``entry(conversation_id, id, context)``.
        id: EntryId
        conversation_id: ConversationId | None
        if context is None:
            id = id_from_number(id_or_conversation_id)
            conversation_id = None
        else:
            if not _is_safe_int(id_or_context):
                raise StorageRequestError("Storage.entry() requires an entry ID")
            id = id_from_number(cast(int, id_or_context))
            conversation_id = id_from_number(id_or_conversation_id)
        conversation: ConversationRecord | None = None
        if conversation_id is not None:
            conversation = await self._read_conversation(conversation_id)
            if conversation is None:
                raise StorageRequestError(f"Unknown conversation: {conversation_id}")
        row = await self._db.get(
            f"SELECT record, commit_seq FROM {self._schema.tables['entries']} WHERE id = ?", id
        )
        if row is None:
            return None
        entry = cast(EntryRecord, _parse_json(row["record"]))
        if conversation is not None:
            upper_entry_id = _MAX_SAFE_INTEGER
            while conversation["id"] != entry["conversationId"]:
                parent = conversation.get("parent")
                if parent is None:
                    return None
                upper_entry_id = min(upper_entry_id, parent["at"])
                parent_conversation = await self._read_conversation(parent["conversationId"])
                if parent_conversation is None:
                    return None
                conversation = parent_conversation
            if entry["id"] > upper_entry_id:
                return None
        return {"entry": entry, "commitSeq": seq_from_number(row["commit_seq"])}

    async def find_latest_head_marker(
        self,
        conversation_id: ConversationId,
        at_or_before_entry_id: EntryId | None,
        context: Context,
    ) -> EntryWithHead | None:
        return await self._admit_read(
            lambda: self._read_latest_head_marker(conversation_id, at_or_before_entry_id)
        )

    async def _read_latest_head_marker(
        self,
        conversation_id: ConversationId,
        at_or_before_entry_id: EntryId | None,
    ) -> EntryWithHead | None:
        conversation = await self._read_conversation(conversation_id)
        if conversation is None:
            raise StorageRequestError(f"Unknown conversation: {conversation_id}")
        entries = self._schema.tables["entries"]
        upper = at_or_before_entry_id
        while True:
            if upper is None:
                row = await self._db.get(
                    f"SELECT record FROM {entries} WHERE conversation_id = ? "
                    "AND head IS NOT NULL ORDER BY id DESC LIMIT 1",
                    conversation["id"],
                )
            else:
                row = await self._db.get(
                    f"SELECT record FROM {entries} WHERE conversation_id = ? "
                    "AND head IS NOT NULL AND id <= ? ORDER BY id DESC LIMIT 1",
                    conversation["id"],
                    upper,
                )
            if row is not None:
                return cast(EntryWithHead, _parse_json(row["record"]))
            parent = conversation.get("parent")
            if parent is None:
                return None
            upper = parent["at"] if upper is None else min(upper, parent["at"])
            parent_conversation = await self._read_conversation(parent["conversationId"])
            if parent_conversation is None:
                return None
            conversation = parent_conversation

    async def scan_entries(
        self,
        query: EntryQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[EntryRecord, Cursor]:
        return await self._admit_read(lambda: self._read_entries(query, limit, cursor))

    async def _read_entries(
        self,
        query: EntryQuery,
        limit: int,
        cursor: Cursor | None,
    ) -> Page[EntryRecord, Cursor]:
        start = scan_start(query.get("order"), cursor, "descending")
        if start.order == "ascending":
            return await self._read_entries_ascending(query, limit, start.after)
        conversation = await self._read_conversation(query["conversationId"])
        if conversation is None:
            raise StorageRequestError(f"Unknown conversation: {query['conversationId']}")
        upper: int | None = query.get("maxEntryId")
        if start.after is not None:
            upper = min(
                _MAX_SAFE_INTEGER if upper is None else upper, start.after - 1
            )
        values: list[EntryRecord] = []
        entries_table = self._schema.tables["entries"]
        while True:
            clauses = ["conversation_id = ?"]
            params: list[SqliteValue] = [conversation["id"]]
            if "minEntryId" in query:
                clauses.append("id >= ?")
                params.append(query["minEntryId"])
            if upper is not None:
                clauses.append("id <= ?")
                params.append(upper)
            params.append(limit + 1 - len(values))
            rows = await self._db.all(
                f"SELECT record FROM {entries_table} WHERE {' AND '.join(clauses)} "
                "ORDER BY id DESC LIMIT ?",
                *params,
            )
            values.extend(cast(EntryRecord, _parse_json(row["record"])) for row in rows)
            parent = conversation.get("parent")
            if len(values) > limit or parent is None:
                break
            upper = parent["at"] if upper is None else min(upper, parent["at"])
            if "minEntryId" in query and upper < query["minEntryId"]:
                break
            parent_conversation = await self._read_conversation(parent["conversationId"])
            if parent_conversation is None:
                break
            conversation = parent_conversation
        return _page(values, limit, "descending")

    async def _read_entries_ascending(
        self,
        query: EntryQuery,
        limit: int,
        after: int | None,
    ) -> Page[EntryRecord, Cursor]:
        """Oldest first: the fork chain's segments from the root conversation forward."""

        segments: list[tuple[ConversationId, int | None]] = []
        conversation = await self._read_conversation(query["conversationId"])
        if conversation is None:
            raise StorageRequestError(f"Unknown conversation: {query['conversationId']}")
        upper: int | None = query.get("maxEntryId")
        while True:
            segments.append((id_from_number(conversation["id"]), upper))
            parent = conversation.get("parent")
            if parent is None:
                break
            upper = parent["at"] if upper is None else min(upper, parent["at"])
            if "minEntryId" in query and upper < query["minEntryId"]:
                break
            parent_conversation = await self._read_conversation(parent["conversationId"])
            if parent_conversation is None:
                break
            conversation = parent_conversation
        lower: int | None = query.get("minEntryId")
        if after is not None:
            lower = max(after + 1 if lower is None else lower, after + 1)
        values: list[EntryRecord] = []
        entries_table = self._schema.tables["entries"]
        for conversation_id, segment_upper in reversed(segments):
            clauses = ["conversation_id = ?"]
            params: list[SqliteValue] = [conversation_id]
            if lower is not None:
                clauses.append("id >= ?")
                params.append(lower)
            if segment_upper is not None:
                clauses.append("id <= ?")
                params.append(segment_upper)
            params.append(limit + 1 - len(values))
            rows = await self._db.all(
                f"SELECT record FROM {entries_table} WHERE {' AND '.join(clauses)} "
                "ORDER BY id ASC LIMIT ?",
                *params,
            )
            values.extend(cast(EntryRecord, _parse_json(row["record"])) for row in rows)
            if len(values) > limit:
                break
        return _page(values, limit, "ascending")

    async def task(self, id: TaskId, context: Context) -> TaskRecord | None:
        self._assert_open()
        row = await self._db.get(
            f"SELECT record FROM {self._schema.tables['tasks']} WHERE id = ?", id
        )
        return None if row is None else cast(TaskRecord, _parse_json(row["record"]))

    async def scan_tasks(
        self,
        query: TaskQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[TaskRecord, Cursor]:
        self._assert_open()
        start = scan_start(query.get("order"), cursor, "ascending")
        clause, param, direction = _scan_sql(start)
        clauses = [clause]
        params: list[SqliteValue] = [param]
        if "conversationId" in query:
            clauses.append("conversation_id = ?")
            params.append(query["conversationId"])
        if "kind" in query:
            clauses.append("kind = ?")
            params.append(_encode_indexed_string(query["kind"]))
        if "status" in query:
            clauses.append("status = ?")
            params.append(query["status"])
        if "abortRequested" in query:
            clauses.append("abort_requested = ?")
            params.append(1 if query["abortRequested"] else 0)
        if "background" in query:
            clauses.append("background = ?")
            params.append(1 if query["background"] else 0)
        params.append(limit + 1)
        rows = await self._db.all(
            f"SELECT record FROM {self._schema.tables['tasks']} "
            f"WHERE {' AND '.join(clauses)} ORDER BY id {direction} LIMIT ?",
            *params,
        )
        return _page([_parse_json(row["record"]) for row in rows], limit, start.order)

    async def submission(self, id: SubmissionId, context: Context) -> SubmissionRecord | None:
        self._assert_open()
        row = await self._db.get(
            f"SELECT record FROM {self._schema.tables['submissions']} WHERE id = ?", id
        )
        return None if row is None else cast(SubmissionRecord, _parse_json(row["record"]))

    async def scan_submissions(
        self,
        query: SubmissionQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[SubmissionRecord, Cursor]:
        self._assert_open()
        start = scan_start(query.get("order"), cursor, "ascending")
        clause, param, direction = _scan_sql(start)
        clauses = [clause]
        params: list[SqliteValue] = [param]
        if "conversationId" in query:
            clauses.append("conversation_id = ?")
            params.append(query["conversationId"])
        if "status" in query:
            clauses.append("status = ?")
            params.append(query["status"])
        params.append(limit + 1)
        rows = await self._db.all(
            f"SELECT record FROM {self._schema.tables['submissions']} "
            f"WHERE {' AND '.join(clauses)} ORDER BY id {direction} LIMIT ?",
            *params,
        )
        return _page([_parse_json(row["record"]) for row in rows], limit, start.order)

    async def submission_by_request(
        self,
        conversation_id: ConversationId,
        request_id: str,
        context: Context,
    ) -> SubmissionRecord | None:
        self._assert_open()
        row = await self._db.get(
            f"SELECT record FROM {self._schema.tables['submissions']} "
            "WHERE conversation_id = ? AND request_id = ?",
            conversation_id,
            _encode_indexed_string(request_id),
        )
        return None if row is None else cast(SubmissionRecord, _parse_json(row["record"]))

    # ── Document reads ──────────────────────────────────────────────────────

    async def find_document(
        self,
        address: DocumentAddress,
        at: DocumentPoint,
        context: Context,
    ) -> DocumentRecord | None:
        self._assert_open()
        parts = _address_parts(address)
        documents = self._schema.tables["documents"]
        if at == "current":
            sql = (
                f"SELECT record FROM {documents} WHERE kind = ? AND scope_kind = ? "
                "AND owner_id = ? AND family = ? AND key_value = ? AND retired_at IS NULL "
                "ORDER BY created_at DESC LIMIT 1"
            )
            params: list[SqliteValue] = [
                parts.kind,
                parts.scope_kind,
                parts.owner_id,
                parts.family,
                parts.key_value,
            ]
        else:
            sql = (
                f"SELECT record FROM {documents} WHERE kind = ? AND scope_kind = ? "
                "AND owner_id = ? AND family = ? AND key_value = ? AND created_at <= ? "
                "AND (retired_at IS NULL OR retired_at > ?) ORDER BY created_at DESC LIMIT 1"
            )
            params = [
                parts.kind,
                parts.scope_kind,
                parts.owner_id,
                parts.family,
                parts.key_value,
                at,
                at,
            ]
        row = await self._db.get(sql, *params)
        return None if row is None else cast(DocumentRecord, _parse_json(row["record"]))

    async def document(
        self,
        id: DocumentId,
        at: DocumentPoint,
        context: Context,
    ) -> StoredDocument | None:
        self._assert_open()
        # The record and revision queries must observe one committed state; a commit between them
        # can replace the base.
        return await self._db.transaction(
            lambda transaction: self._materialize_document(transaction, id, at)
        )

    async def scan_documents(
        self,
        query: DocumentQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[DocumentRecord, Cursor]:
        self._assert_open()
        scope = _scope_columns(query["scope"])
        after = _cursor_id(cursor)
        clauses = ["scope_kind = ?", "owner_id = ?", "id > ?"]
        params: list[SqliteValue] = [
            scope.scope_kind,
            scope.owner_id,
            -1 if after is None else cast(int, after),
        ]
        if "kind" in query:
            clauses.append("kind = ?")
            params.append(_encode_indexed_string(query["kind"]))
        if query["at"] == "current":
            clauses.append("retired_at IS NULL")
        else:
            clauses.append("created_at <= ?")
            clauses.append("(retired_at IS NULL OR retired_at > ?)")
            params.append(cast(int, query["at"]))
            params.append(cast(int, query["at"]))
        params.append(limit + 1)
        rows = await self._db.all(
            f"SELECT record FROM {self._schema.tables['documents']} "
            f"WHERE {' AND '.join(clauses)} ORDER BY id LIMIT ?",
            *params,
        )
        return _page([_parse_json(row["record"]) for row in rows], limit)

    def close(self, context: Context) -> asyncio.Future[None]:
        if self._closing is None:
            self._closed = True
            self._closing = asyncio.ensure_future(self._close_database())
        return self._closing

    # ── Reads and close helpers ─────────────────────────────────────────────

    async def _close_database(self) -> None:
        if self._admitted_reads > 0:
            self._reads_drained = asyncio.get_running_loop().create_future()
            await self._reads_drained
        await self._db.close()

    async def _admit_read(self, read: Callable[[], Awaitable[T]]) -> T:
        """Run a grouped read; close waits for admitted reads so none reaches a closed database."""

        self._assert_open()
        self._admitted_reads += 1
        try:
            return await read()
        finally:
            self._admitted_reads -= 1
            if (
                self._admitted_reads == 0
                and self._reads_drained is not None
                and not self._reads_drained.done()
            ):
                self._reads_drained.set_result(None)

    async def _read_conversation(self, id: ConversationId) -> ConversationRecord | None:
        row = await self._db.get(
            f"SELECT record FROM {self._schema.tables['conversations']} WHERE id = ?", id
        )
        return None if row is None else cast(ConversationRecord, _parse_json(row["record"]))

    async def _materialize_document(
        self,
        executor: SqliteExecutor,
        id: DocumentId,
        at: DocumentPoint,
    ) -> StoredDocument | None:
        documents = self._schema.tables["documents"]
        revisions = self._schema.tables["documentRevisions"]
        row = await executor.get(f"SELECT record FROM {documents} WHERE id = ?", id)
        if row is None:
            return None
        record = cast(DocumentRecord, _parse_json(row["record"]))
        if at != "current" and _is_current_only(record):
            raise StorageRequestError(f"Document {id} does not retain historical content")
        if not _is_alive_at(record, at):
            return None
        upper = _MAX_SAFE_INTEGER if at == "current" else at
        base = await executor.get(
            f"SELECT seq, kind, version, content FROM {revisions} WHERE document_id = ? "
            "AND kind = 'base' AND seq <= ? ORDER BY seq DESC LIMIT 1",
            id,
            upper,
        )
        if base is None:
            raise RuntimeError(f"Document {id} is missing a required base")
        value: JsonObject = _parse_json(base["content"])
        tail = await executor.all(
            f"SELECT seq, kind, version, content FROM {revisions} WHERE document_id = ? "
            "AND seq > ? AND seq <= ? ORDER BY seq",
            id,
            base["seq"],
            upper,
        )
        for revision in tail:
            if revision["kind"] != "delta" or revision["version"] != base["version"]:
                raise RuntimeError(
                    f"Document {id} crosses a stored version boundary without a base"
                )
            value = cast(JsonObject, apply(value, _parse_json(revision["content"])))
        return {
            "record": record,
            "version": base["version"],
            "value": value,
            "deltasSinceBase": len(tail),
        }

    # ── Commit validation and application ───────────────────────────────────

    def _candidate_next_id(self, writes: Sequence[StorageWrite]) -> int:
        next_id = self._next_id
        for write in writes:
            id = _write_id(write)
            if id is not None:
                next_id = max(next_id, id + 1)
        return next_id

    async def _check_global_ids(
        self,
        executor: SqliteExecutor,
        writes: Sequence[StorageWrite],
    ) -> None:
        record_ids = self._schema.tables["recordIds"]
        claimed: dict[int, str] = {}
        for write in writes:
            kind = write["type"]
            if kind in ("document.change", "document.retire"):
                continue
            is_document = kind in ("document.create", "document.copy")
            table = "document" if is_document else kind
            id = _write_id(write)
            assert id is not None
            row = await executor.get(
                f"SELECT record_type FROM {record_ids} WHERE id = ?", id
            )
            existing = None if row is None else row["record_type"]
            earlier = claimed.get(id)
            if table in ("conversation", "entry", "document"):
                if existing is not None:
                    raise RuntimeError(f"ID {id} already belongs to {existing}")
                if earlier is not None:
                    raise RuntimeError(f"ID {id} is written more than once")
            else:
                if existing is not None and existing != table:
                    raise RuntimeError(f"ID {id} already belongs to {existing}")
                if earlier is not None and earlier != table:
                    raise RuntimeError(f"ID {id} is written as two record types")
            claimed[id] = table

    def _prepare_document_actions(
        self,
        writes: Sequence[StorageWrite],
    ) -> dict[DocumentId, _DocumentAction]:
        actions: dict[DocumentId, _DocumentAction] = {}
        for write in writes:
            item = cast(dict[str, Any], write)
            kind = item["type"]
            if kind not in (
                "document.create",
                "document.copy",
                "document.change",
                "document.retire",
            ):
                continue
            id = item["record"]["id"] if kind in ("document.create", "document.copy") else item["id"]
            action = actions.get(id)
            if action is None:
                action = _DocumentAction()
                actions[id] = action
            if kind in ("document.create", "document.copy"):
                if (
                    action.create is not None
                    or action.content is not None
                    or action.copy is not None
                ):
                    raise RuntimeError(f"Document {id} has more than one content command")
                action.create = item["record"]
                if kind == "document.create":
                    action.content = item["content"]
                else:
                    action.copy = dict(item["source"])
            elif kind == "document.change":
                if action.content is not None or action.copy is not None:
                    raise RuntimeError(f"Document {id} has more than one content command")
                action.content = item["content"]
            else:
                if action.retire:
                    raise RuntimeError(f"Document {id} is retired more than once")
                action.retire = True
        return actions

    async def _check_document_actions(
        self,
        executor: SqliteExecutor,
        actions: dict[DocumentId, _DocumentAction],
    ) -> None:
        documents = self._schema.tables["documents"]
        revisions = self._schema.tables["documentRevisions"]
        live_counts: dict[str, int] = {}
        for id, action in actions.items():
            if action.copy is not None and action.copy["id"] in actions:
                raise RuntimeError(f"Document copy {id} source is changed in the copy batch")
            row = await executor.get(f"SELECT record FROM {documents} WHERE id = ?", id)
            existing = None if row is None else cast(DocumentRecord, _parse_json(row["record"]))
            if action.create is None and existing is None:
                raise RuntimeError(f"Unknown document: {id}")
            if action.create is not None and existing is not None:
                raise RuntimeError(f"Document {id} already exists")
            if existing is not None and "retiredAt" in existing:
                raise RuntimeError(f"Document {id} is retired")
            if action.content is not None and action.content["kind"] == "delta":
                previous = await executor.get(
                    f"SELECT version FROM {revisions} WHERE document_id = ? "
                    "ORDER BY seq DESC LIMIT 1",
                    id,
                )
                if previous is None:
                    raise RuntimeError(f"Document {id} delta has no base")
                if previous["version"] != action.content["version"]:
                    raise RuntimeError(f"Document {id} version transition requires a base")
            record = action.create if action.create is not None else existing
            assert record is not None
            key = _address_key(record)
            live = live_counts.get(key)
            if live is None:
                current = await self._current_document_id(executor, record)
                live = 0 if current is None else 1
            if action.retire and existing is not None:
                live -= 1
            if action.create is not None and not action.retire:
                live += 1
            live_counts[key] = live
        for live in live_counts.values():
            if live > 1:
                raise RuntimeError("Document address already has a current incarnation")

    async def _current_document_id(
        self,
        executor: SqliteExecutor,
        address: DocumentAddress | DocumentCreate | DocumentRecord,
    ) -> DocumentId | None:
        parts = _address_parts(address)
        row = await executor.get(
            f"SELECT id FROM {self._schema.tables['documents']} WHERE kind = ? "
            "AND scope_kind = ? AND owner_id = ? AND family = ? AND key_value = ? "
            "AND retired_at IS NULL LIMIT 1",
            parts.kind,
            parts.scope_kind,
            parts.owner_id,
            parts.family,
            parts.key_value,
        )
        return None if row is None else id_from_number(row["id"])

    async def _apply_table_write(
        self,
        executor: SqliteExecutor,
        write: StorageWrite,
        seq: Seq,
    ) -> None:
        tables = self._schema.tables
        item = cast(dict[str, Any], write)
        kind = item["type"]
        if kind == "conversation":
            value = item["value"]
            await self._claim_id(executor, value["id"], "conversation")
            owner = value.get("owner")
            await executor.run(
                f"INSERT INTO {tables['conversations']} "
                "(id, owner_conversation_id, owner_task_id, record) VALUES (?, ?, ?, ?)",
                value["id"],
                None if owner is None else owner["conversationId"],
                None if owner is None else owner["taskId"],
                _encode_json(value),
            )
        elif kind == "entry":
            value = item["value"]
            await self._claim_id(executor, value["id"], "entry")
            await executor.run(
                f"INSERT INTO {tables['entries']} "
                "(id, conversation_id, head, commit_seq, record) VALUES (?, ?, ?, ?, ?)",
                value["id"],
                value["conversationId"],
                value.get("head"),
                seq,
                _encode_json(value),
            )
        elif kind == "task":
            value = item["value"]
            await self._claim_id(executor, value["id"], "task")
            await executor.run(
                f"INSERT INTO {tables['tasks']} "
                "(id, conversation_id, kind, status, abort_requested, background, record) "
                "VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET conversation_id = excluded.conversation_id, "
                "kind = excluded.kind, status = excluded.status, "
                "abort_requested = excluded.abort_requested, "
                "background = excluded.background, record = excluded.record",
                value["id"],
                value["conversationId"],
                _encode_indexed_string(value["kind"]),
                value["state"]["status"],
                1 if value["abortRequested"] else 0,
                1 if value["background"] else 0,
                _encode_json(value),
            )
        elif kind == "submission":
            value = item["value"]
            await self._claim_id(executor, value["id"], "submission")
            request_id = value.get("requestId")
            await executor.run(
                f"INSERT INTO {tables['submissions']} "
                "(id, conversation_id, request_id, status, record) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET conversation_id = excluded.conversation_id, "
                "request_id = excluded.request_id, status = excluded.status, "
                "record = excluded.record",
                value["id"],
                value["conversationId"],
                None if request_id is None else _encode_indexed_string(request_id),
                value["status"],
                _encode_json(value),
            )

    async def _claim_id(
        self,
        executor: SqliteExecutor,
        id: int,
        table: str,
    ) -> None:
        await executor.run(
            f"INSERT OR IGNORE INTO {self._schema.tables['recordIds']} "
            "(id, record_type) VALUES (?, ?)",
            id,
            table,
        )

    async def _apply_document_actions(
        self,
        executor: SqliteExecutor,
        actions: dict[DocumentId, _DocumentAction],
        seq: Seq,
    ) -> None:
        tables = self._schema.tables
        documents = tables["documents"]
        revisions = tables["documentRevisions"]
        for id, action in actions.items():
            content = action.content
            if action.copy is not None:
                try:
                    stored = await self._materialize_document(
                        executor,
                        action.copy["id"],
                        action.copy["at"],
                    )
                    if stored is None:
                        raise RuntimeError(
                            f"Fork source document {action.copy['id']} cannot be read"
                        )
                    create = action.create
                    assert create is not None
                    source_record = stored["record"]
                    if (
                        source_record["scope"]["kind"] != "conversation"
                        or create["scope"]["kind"] != "conversation"
                        or source_record["kind"] != create["kind"]
                        or source_record.get("key") != create.get("key")
                        or source_record.get("history") != create.get("history")
                        or source_record.get("fork") != create.get("fork")
                    ):
                        raise RuntimeError(
                            f"Fork source document {action.copy['id']} "
                            "does not match the copied record"
                        )
                    content = {
                        "kind": "base",
                        "version": stored["version"],
                        "value": stored["value"],
                    }
                except Exception as error:
                    raise RuntimeError(f"Document copy {id} was rejected") from error
            if action.create is not None:
                record = cast(
                    DocumentRecord,
                    {
                        **action.create,
                        "createdAt": seq,
                        **({"retiredAt": seq} if action.retire else {}),
                    },
                )
                parts = _address_parts(record)
                await self._claim_id(executor, id, "document")
                await executor.run(
                    f"INSERT INTO {documents} (id, kind, family, key_value, scope_kind, "
                    "owner_id, created_at, retired_at, record) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    id,
                    parts.kind,
                    parts.family,
                    parts.key_value,
                    parts.scope_kind,
                    parts.owner_id,
                    seq,
                    seq if action.retire else None,
                    _encode_json(record),
                )
            else:
                row = await executor.get(f"SELECT record FROM {documents} WHERE id = ?", id)
                assert row is not None
                record = cast(DocumentRecord, _parse_json(row["record"]))
            if content is not None:
                if content["kind"] == "base" and _is_current_only(record):
                    await executor.run(
                        f"DELETE FROM {revisions} WHERE document_id = ?", id
                    )
                encoded = (
                    _encode_json(content["value"])
                    if content["kind"] == "base"
                    else _encode_json(content["ops"])
                )
                await executor.run(
                    f"INSERT INTO {revisions} (document_id, seq, kind, version, content) "
                    "VALUES (?, ?, ?, ?, ?)",
                    id,
                    seq,
                    content["kind"],
                    content["version"],
                    encoded,
                )
            if action.retire:
                if action.create is None:
                    record = cast(DocumentRecord, {**record, "retiredAt": seq})
                    await executor.run(
                        f"UPDATE {documents} SET retired_at = ?, record = ? WHERE id = ?",
                        seq,
                        _encode_json(record),
                        id,
                    )
                if _is_current_only(record):
                    await executor.run(
                        f"DELETE FROM {revisions} WHERE document_id = ?", id
                    )

    def _assert_open(self) -> None:
        if self._closed:
            raise RuntimeError("SqliteStorage is closed")


__all__ = ["SqliteStorage", "SqliteStorageOptions"]
