"""Detached in-memory reference storage.

Reads and retained writes are copied intentionally, matching the ownership boundary of
serialization-backed stores. This is backend conformance, not defensive validation: storage still
trusts Session-supplied semantics such as ancestry and ownership.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, cast, overload

from ..context import Context
from ..delta import Op, apply_immutable_batches
from ..documents import address_id
from ..errors import StorageRequestError
from ..ids import (
    ConversationId,
    DocumentId,
    EntryId,
    Seq,
    SubmissionId,
    TaskId,
    id_from_number,
    seq_from_number,
)
from ..json import JsonObject, JsonValue, copy_json, freeze_json
from ..types import (
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
from .scan import next_cursor, scan_start

_MAX_SAFE_INTEGER = 9_007_199_254_740_991
_TASK_STATUSES = ("pending", "running", "waiting", "completing", "terminal")
_SUBMISSION_STATUSES = ("queued", "placed", "done", "unanswered")


@dataclass(slots=True)
class DocumentRevision:
    seq: Seq
    content: DocumentContent


@dataclass(slots=True)
class StoredDocumentState:
    record: DocumentRecord
    revisions: list[DocumentRevision]


@dataclass(slots=True)
class DocumentAction:
    create: DocumentCreate | None = None
    content: DocumentContent | None = None
    retire: bool = False


@dataclass(slots=True)
class DocumentAddressIndex:
    ids: list[DocumentId] = field(default_factory=list)
    current_id: DocumentId | None = None


def _is_safe_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and abs(value) <= _MAX_SAFE_INTEGER


def _cursor_id(cursor: Cursor | None) -> DocumentId | None:
    if cursor is None:
        return None
    after = cursor.get("after")
    if not _is_safe_int(after):
        raise StorageRequestError("Invalid storage cursor")
    return id_from_number(cast(int, after))


def _lower_bound(ids: Sequence[int], target: int) -> int:
    low, high = 0, len(ids)
    while low < high:
        middle = (low + high) // 2
        if ids[middle] < target:
            low = middle + 1
        else:
            high = middle
    return low


def _upper_bound(ids: Sequence[int], target: int) -> int:
    low, high = 0, len(ids)
    while low < high:
        middle = (low + high) // 2
        if ids[middle] <= target:
            low = middle + 1
        else:
            high = middle
    return low


def _insert_sorted(ids: list[Any], id: Any) -> None:
    if not ids or ids[-1] < id:
        ids.append(id)
    else:
        ids.insert(_lower_bound(ids, id), id)


def _remove_sorted(ids: list[Any], id: Any) -> None:
    index = _lower_bound(ids, id)
    if index < len(ids) and ids[index] == id:
        ids.pop(index)


def _insert_map_id(index: dict[Any, list[Any]], key: Any, id: Any) -> None:
    ids = index.setdefault(key, [])
    _insert_sorted(ids, id)


def _scope_key(scope: dict[str, Any]) -> str:
    if scope["kind"] == "session":
        return '["session"]'
    if scope["kind"] == "conversation":
        return f'["conversation",{scope["conversationId"]}]'
    return f'["task",{scope["taskId"]}]'


def _address_key(address: DocumentAddress) -> str:
    return address_id(address)


def _record_address_key(record: DocumentCreate | DocumentRecord) -> str:
    key = {"key": record["key"]} if "key" in record else {}
    return _address_key(cast(DocumentAddress, {"kind": record["kind"], "scope": record["scope"], **key}))


def _is_alive_at(record: DocumentRecord, at: DocumentPoint) -> bool:
    if at == "current":
        return "retiredAt" not in record
    return record["createdAt"] <= at and ("retiredAt" not in record or at < record["retiredAt"])


def _is_current_only(record: DocumentRecord) -> bool:
    return record["scope"]["kind"] != "conversation" or record.get("history") == "latest"


def _scan_indexes(ids: Sequence[int], order: ScanOrder, after: int | None) -> Iterable[int]:
    if order == "ascending":
        first = 0 if after is None else _upper_bound(ids, after)
        yield from range(first, len(ids))
    else:
        end = len(ids) if after is None else _lower_bound(ids, after)
        yield from range(end - 1, -1, -1)


def _page(values: Sequence[Any], limit: int, order: ScanOrder) -> Page[Any, Cursor]:
    if not values:
        return {"items": []}
    items = [copy_json(item) for item in values[:limit]]
    if len(values) <= limit:
        return {"items": items}
    return {"items": items, "next": next_cursor(items[-1]["id"], order)}


@dataclass(slots=True)
class _State:
    record_types: dict[int, str] = field(default_factory=dict)
    conversations: dict[ConversationId, ConversationRecord] = field(default_factory=dict)
    conversation_ids: list[ConversationId] = field(default_factory=list)
    conversation_ids_by_owner_conversation: dict[ConversationId, list[ConversationId]] = field(
        default_factory=dict
    )
    conversation_ids_by_owner_task: dict[TaskId, list[ConversationId]] = field(default_factory=dict)
    entries: dict[EntryId, EntryRecord] = field(default_factory=dict)
    entry_ids: dict[ConversationId, list[EntryId]] = field(default_factory=dict)
    head_entry_ids: dict[ConversationId, list[EntryId]] = field(default_factory=dict)
    entry_commit_seqs: dict[EntryId, Seq] = field(default_factory=dict)
    tasks: dict[TaskId, TaskRecord] = field(default_factory=dict)
    task_ids: list[TaskId] = field(default_factory=list)
    task_ids_by_status: dict[str, list[TaskId]] = field(
        default_factory=lambda: {status: [] for status in _TASK_STATUSES}
    )
    submissions: dict[SubmissionId, SubmissionRecord] = field(default_factory=dict)
    submission_ids: list[SubmissionId] = field(default_factory=list)
    submission_ids_by_status: dict[str, list[SubmissionId]] = field(
        default_factory=lambda: {status: [] for status in _SUBMISSION_STATUSES}
    )
    submission_ids_by_request: dict[ConversationId, dict[str, SubmissionId]] = field(default_factory=dict)
    documents: dict[DocumentId, StoredDocumentState] = field(default_factory=dict)
    document_addresses: dict[str, DocumentAddressIndex] = field(default_factory=dict)
    document_ids_by_scope: dict[str, list[DocumentId]] = field(default_factory=dict)


class PreparedMemoryCommit:
    """A fully validated, detached state mutation whose application performs no fallible preparation."""

    __slots__ = ("_apply", "_applied", "seq", "writes")

    def __init__(
        self,
        seq: Seq,
        writes: Sequence[StorageWrite],
        apply: Any,
    ) -> None:
        self.seq = seq
        self.writes = writes
        self._apply = apply
        self._applied = False

    def apply(self) -> Seq:
        if not self._applied:
            self._applied = True
            self._apply()
        return self.seq


class MemoryStorage:
    """Detached in-memory reference implementation of :class:`~pi_durable.types.Storage`."""

    def __init__(self) -> None:
        self._state = _State()
        self._next_id = 2
        self._next_seq = 1
        self._closed = False

    async def commit(self, writes: Sequence[StorageWrite], context: Context) -> Seq:
        return self.prepare_commit(writes).apply()

    def prepare_commit(
        self,
        writes: Sequence[StorageWrite],
        seq: Seq | None = None,
    ) -> PreparedMemoryCommit:
        """Validate and detach one commit without changing observable state."""

        self._assert_open()
        assigned = seq_from_number(self._next_seq) if seq is None else seq
        if not _is_safe_int(assigned) or assigned < self._next_seq:
            raise ValueError(f"Commit sequence {assigned} does not strictly increase")
        detached = cast(
            list[StorageWrite],
            copy_json(
                self._resolve_document_copies(
                    [cast(StorageWrite, copy_json(write)) for write in writes]
                )
            ),
        )
        frozen = cast(
            list[StorageWrite],
            freeze_json(detached),
        )
        self._check_global_ids(frozen)
        document_actions = self._prepare_document_actions(frozen)
        self._check_document_actions(document_actions)

        def apply() -> None:
            self._apply_prepared_commit(frozen, document_actions, assigned)

        return PreparedMemoryCommit(assigned, frozen, apply)

    # ── Validation and preparation ─────────────────────────────────────────

    def _resolve_document_copies(self, writes: list[StorageWrite]) -> list[StorageWrite]:
        if not any(write["type"] == "document.copy" for write in writes):
            return writes
        changed_ids: set[int] = set()
        for write in writes:
            if write["type"] in ("document.create", "document.copy"):
                changed_ids.add(write["record"]["id"])
            elif write["type"] in ("document.change", "document.retire"):
                changed_ids.add(write["id"])
        resolved: list[StorageWrite] = []
        for write in writes:
            if write["type"] != "document.copy":
                resolved.append(write)
                continue
            try:
                source = write["source"]
                if source["id"] in changed_ids:
                    raise ValueError(f"Fork source document {source['id']} is changed in the copy batch")
                stored = self._materialize_document(source["id"], source["at"])
                if stored is None:
                    raise ValueError(f"Fork source document {source['id']} cannot be read")
                source_record = stored["record"]
                record = write["record"]
                if (
                    source_record["scope"]["kind"] != "conversation"
                    or record["scope"]["kind"] != "conversation"
                    or source_record["kind"] != record["kind"]
                    or source_record.get("key") != record.get("key")
                    or source_record.get("history") != record.get("history")
                    or source_record.get("fork") != record.get("fork")
                ):
                    raise ValueError(f"Fork source document {source['id']} does not match the copied record")
                resolved.append(
                    cast(
                        StorageWrite,
                        {
                            "type": "document.create",
                            "record": record,
                            "content": {
                                "kind": "base",
                                "version": stored["version"],
                                "value": stored["value"],
                            },
                        },
                    )
                )
            except Exception as error:
                raise ValueError(f"Document copy {write['record']['id']} was rejected") from error
        return resolved

    def _check_global_ids(self, writes: Sequence[StorageWrite]) -> None:
        claimed: dict[int, str] = {}
        for write in writes:
            if write["type"] in ("document.change", "document.retire"):
                continue
            if write["type"] in ("document.create", "document.copy"):
                table = "document"
                id = write["record"]["id"]
            else:
                table = write["type"]
                id = write["value"]["id"]
            existing = self._state.record_types.get(id)
            earlier = claimed.get(id)
            if table in ("conversation", "entry", "document"):
                if existing is not None:
                    raise ValueError(f"ID {id} already belongs to {existing}")
                if earlier is not None:
                    raise ValueError(f"ID {id} is written more than once")
            else:
                if existing is not None and existing != table:
                    raise ValueError(f"ID {id} already belongs to {existing}")
                if earlier is not None and earlier != table:
                    raise ValueError(f"ID {id} is written as two record types")
            claimed[id] = table

    def _prepare_document_actions(self, writes: Sequence[StorageWrite]) -> dict[DocumentId, DocumentAction]:
        actions: dict[DocumentId, DocumentAction] = {}
        for write in writes:
            if write["type"] not in ("document.create", "document.change", "document.retire"):
                continue
            id = write["record"]["id"] if write["type"] == "document.create" else write["id"]
            action = actions.setdefault(id, DocumentAction())
            if write["type"] == "document.create":
                if action.create is not None or action.content is not None:
                    raise ValueError(f"Document {id} has more than one content command")
                action.create = write["record"]
                action.content = write["content"]
            elif write["type"] == "document.change":
                if action.content is not None:
                    raise ValueError(f"Document {id} has more than one content command")
                action.content = write["content"]
            else:
                if action.retire:
                    raise ValueError(f"Document {id} is retired more than once")
                action.retire = True
        return actions

    def _check_document_actions(self, actions: dict[DocumentId, DocumentAction]) -> None:
        live_counts: dict[str, int] = {}
        for id, action in actions.items():
            existing = self._state.documents.get(id)
            if action.create is None and existing is None:
                raise ValueError(f"Unknown document: {id}")
            if action.create is not None and existing is not None:
                raise ValueError(f"Document {id} already exists")
            if existing is not None and "retiredAt" in existing.record:
                raise ValueError(f"Document {id} is retired")
            previous = existing.revisions[-1] if existing is not None and existing.revisions else None
            if action.content is not None and action.content["kind"] == "delta":
                if previous is None:
                    raise ValueError(f"Document {id} delta has no base")
                if previous.content.get("version") != action.content["version"]:
                    raise ValueError(f"Document {id} version transition requires a base")
            key = _record_address_key(action.create if action.create is not None else existing.record)
            current_id = self._state.document_addresses.get(key, DocumentAddressIndex()).current_id
            live = live_counts.get(key)
            if live is None:
                live = 0 if current_id is None else 1
            if action.retire and current_id == id:
                live -= 1
            if action.create is not None and not action.retire:
                live += 1
            live_counts[key] = live
        for live in live_counts.values():
            if live > 1:
                raise ValueError("Document address already has a current incarnation")

    # ── Application ─────────────────────────────────────────────────────────

    def _apply_prepared_commit(
        self,
        writes: Sequence[StorageWrite],
        document_actions: dict[DocumentId, DocumentAction],
        seq: Seq,
    ) -> None:
        state = self._state
        for write in writes:
            item = cast(dict[str, Any], write)
            kind = item["type"]
            if kind == "conversation":
                value = item["value"]
                state.record_types[value["id"]] = "conversation"
                state.conversations[value["id"]] = value
                _insert_sorted(state.conversation_ids, value["id"])
                owner = value.get("owner")
                if owner is not None:
                    _insert_map_id(
                        state.conversation_ids_by_owner_conversation,
                        owner["conversationId"],
                        value["id"],
                    )
                    _insert_map_id(state.conversation_ids_by_owner_task, owner["taskId"], value["id"])
                self._next_id = max(self._next_id, value["id"] + 1)
            elif kind == "entry":
                value = item["value"]
                state.record_types[value["id"]] = "entry"
                state.entries[value["id"]] = value
                state.entry_commit_seqs[value["id"]] = seq
                ids = state.entry_ids.setdefault(value["conversationId"], [])
                _insert_sorted(ids, value["id"])
                if "head" in value:
                    heads = state.head_entry_ids.setdefault(value["conversationId"], [])
                    _insert_sorted(heads, value["id"])
                self._next_id = max(self._next_id, value["id"] + 1)
            elif kind == "task":
                value = item["value"]
                state.record_types[value["id"]] = "task"
                previous = state.tasks.get(value["id"])
                if previous is None:
                    _insert_sorted(state.task_ids, value["id"])
                    _insert_sorted(state.task_ids_by_status[value["state"]["status"]], value["id"])
                elif previous["state"]["status"] != value["state"]["status"]:
                    _remove_sorted(state.task_ids_by_status[previous["state"]["status"]], value["id"])
                    _insert_sorted(state.task_ids_by_status[value["state"]["status"]], value["id"])
                state.tasks[value["id"]] = value
                self._next_id = max(self._next_id, value["id"] + 1)
            elif kind == "submission":
                value = item["value"]
                state.record_types[value["id"]] = "submission"
                previous = state.submissions.get(value["id"])
                if previous is None:
                    _insert_sorted(state.submission_ids, value["id"])
                    _insert_sorted(state.submission_ids_by_status[value["status"]], value["id"])
                elif previous["status"] != value["status"]:
                    _remove_sorted(state.submission_ids_by_status[previous["status"]], value["id"])
                    _insert_sorted(state.submission_ids_by_status[value["status"]], value["id"])
                previous_request = previous.get("requestId") if previous is not None else None
                if previous_request is not None:
                    requests = state.submission_ids_by_request.get(previous["conversationId"])
                    if requests is not None and requests.get(previous_request) == value["id"]:
                        requests.pop(previous_request, None)
                        if not requests:
                            state.submission_ids_by_request.pop(previous["conversationId"], None)
                state.submissions[value["id"]] = value
                request = value.get("requestId")
                if request is not None:
                    requests = state.submission_ids_by_request.setdefault(value["conversationId"], {})
                    requests[request] = value["id"]
                self._next_id = max(self._next_id, value["id"] + 1)
            elif kind == "document.copy":
                raise ValueError("Prepared document copy was not resolved")
        self._apply_document_actions(document_actions, seq)
        self._next_seq = seq + 1

    def _apply_document_actions(self, actions: dict[DocumentId, DocumentAction], seq: Seq) -> None:
        state = self._state
        for id, action in actions.items():
            stored = state.documents.get(id)
            if action.create is not None:
                record: DocumentRecord = cast(
                    DocumentRecord,
                    {
                        **action.create,
                        "createdAt": seq,
                        **({"retiredAt": seq} if action.retire else {}),
                    },
                )
                assert action.content is not None
                stored = StoredDocumentState(record, [DocumentRevision(seq, action.content)])
                state.record_types[id] = "document"
                state.documents[id] = stored
                key = _record_address_key(record)
                address = state.document_addresses.setdefault(key, DocumentAddressIndex())
                _insert_sorted(address.ids, id)
                scope_ids = state.document_ids_by_scope.setdefault(_scope_key(record["scope"]), [])
                _insert_sorted(scope_ids, id)
                self._next_id = max(self._next_id, id + 1)
            elif action.content is not None and stored is not None:
                revision = DocumentRevision(seq, action.content)
                if action.content["kind"] == "base" and _is_current_only(stored.record):
                    stored.revisions = [revision]
                else:
                    stored.revisions.append(revision)
            if action.retire and action.create is None and stored is not None:
                stored.record = cast(DocumentRecord, {**stored.record, "retiredAt": seq})
            if action.retire and stored is not None and _is_current_only(stored.record):
                stored.revisions = []
            if stored is not None and (action.create is not None or action.retire):
                address = state.document_addresses[_record_address_key(stored.record)]
                if action.retire and address.current_id == id:
                    address.current_id = None
                if action.create is not None and not action.retire:
                    address.current_id = id

    # ── Reads ───────────────────────────────────────────────────────────────

    async def mint_id(self) -> int:
        self._assert_open()
        if not _is_safe_int(self._next_id):
            raise ValueError("ID space is exhausted")
        value = self._next_id
        self._next_id += 1
        return value

    async def conversation(self, id: ConversationId, context: Context) -> ConversationRecord | None:
        self._assert_open()
        value = self._state.conversations.get(id)
        return None if value is None else cast(ConversationRecord, copy_json(value))

    async def scan_conversations(
        self,
        query: ConversationQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[ConversationRecord, Cursor]:
        self._assert_open()
        owner_task = query.get("ownerTaskId")
        owner_conversation = query.get("ownerConversationId")
        if owner_task is not None:
            ids: Sequence[ConversationId] = self._state.conversation_ids_by_owner_task.get(owner_task, [])
        elif owner_conversation is not None:
            ids = self._state.conversation_ids_by_owner_conversation.get(owner_conversation, [])
        else:
            ids = self._state.conversation_ids
        start = scan_start(query.get("order"), cursor, "ascending")
        values: list[ConversationRecord] = []
        for index in _scan_indexes(ids, start.order, start.after):
            if len(values) > limit:
                break
            value = self._state.conversations[ids[index]]
            if owner_conversation is not None:
                owner = value.get("owner")
                if owner is None or owner["conversationId"] != owner_conversation:
                    continue
            values.append(value)
        return _page(values, limit, start.order)

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
        id_or_conversation_id: EntryId | ConversationId | int,
        id_or_context: EntryId | Context | None = None,
        context: Context | None = None,
    ) -> EntryLookup | None:
        self._assert_open()
        if context is None:
            id = id_from_number(id_or_conversation_id)
            entry = self._state.entries.get(id)
            if entry is None:
                return None
            return {
                "entry": cast(EntryRecord, copy_json(entry)),
                "commitSeq": self._state.entry_commit_seqs[id],
            }
        if not _is_safe_int(id_or_context):
            raise StorageRequestError("Storage.entry() requires an entry ID")
        conversation_id = id_from_number(id_or_conversation_id)
        id = id_from_number(id_or_context)
        entry = next(self._visible_entries(conversation_id, id, id), None)
        if entry is None:
            return None
        return {"entry": cast(EntryRecord, copy_json(entry)), "commitSeq": self._state.entry_commit_seqs[id]}

    async def find_latest_head_marker(
        self,
        conversation_id: ConversationId,
        at_or_before_entry_id: EntryId | None,
        context: Context,
    ) -> EntryWithHead | None:
        self._assert_open()
        if conversation_id not in self._state.conversations:
            raise StorageRequestError(f"Unknown conversation: {conversation_id}")
        current_id = conversation_id
        upper = _MAX_SAFE_INTEGER if at_or_before_entry_id is None else at_or_before_entry_id
        while True:
            ids = self._state.head_entry_ids.get(current_id, [])
            index = _upper_bound(ids, upper) - 1
            if index >= 0:
                entry = self._state.entries[ids[index]]
                value = dict(entry)
                value["head"] = entry["head"]
                return cast(EntryWithHead, copy_json(value))
            conversation = self._state.conversations[current_id]
            parent = conversation.get("parent")
            if parent is None:
                return None
            upper = min(upper, parent["at"])
            current_id = parent["conversationId"]

    async def scan_entries(
        self,
        query: EntryQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[EntryRecord, Cursor]:
        self._assert_open()
        start = scan_start(query.get("order"), cursor, "descending")
        min_entry_id: int | None = query.get("minEntryId")
        max_entry_id: int | None = query.get("maxEntryId")
        if start.after is not None and start.order == "descending":
            max_entry_id = min(max_entry_id if max_entry_id is not None else start.after, start.after - 1)
        if start.after is not None and start.order == "ascending":
            min_entry_id = max(min_entry_id if min_entry_id is not None else start.after, start.after + 1)
        visible: list[EntryRecord] = []
        entries = (
            self._visible_entries(query["conversationId"], min_entry_id, max_entry_id)
            if start.order == "descending"
            else self._visible_entries_ascending(query["conversationId"], min_entry_id, max_entry_id)
        )
        for entry in entries:
            visible.append(entry)
            if len(visible) > limit:
                break
        return _page(visible, limit, start.order)

    async def task(self, id: TaskId, context: Context) -> TaskRecord | None:
        self._assert_open()
        value = self._state.tasks.get(id)
        return None if value is None else cast(TaskRecord, copy_json(value))

    async def scan_tasks(
        self,
        query: TaskQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[TaskRecord, Cursor]:
        self._assert_open()
        start = scan_start(query.get("order"), cursor, "ascending")
        status = query.get("status")
        ids: Sequence[TaskId] = (
            self._state.task_ids if status is None else self._state.task_ids_by_status[status]
        )
        values: list[TaskRecord] = []
        for index in _scan_indexes(ids, start.order, start.after):
            if len(values) > limit:
                break
            value = self._state.tasks[ids[index]]
            if "conversationId" in query and value["conversationId"] != query["conversationId"]:
                continue
            if "kind" in query and value["kind"] != query["kind"]:
                continue
            if "abortRequested" in query and value["abortRequested"] != query["abortRequested"]:
                continue
            if "background" in query and value["background"] != query["background"]:
                continue
            values.append(value)
        return _page(values, limit, start.order)

    async def submission(self, id: SubmissionId, context: Context) -> SubmissionRecord | None:
        self._assert_open()
        value = self._state.submissions.get(id)
        return None if value is None else cast(SubmissionRecord, copy_json(value))

    async def scan_submissions(
        self,
        query: SubmissionQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[SubmissionRecord, Cursor]:
        self._assert_open()
        start = scan_start(query.get("order"), cursor, "ascending")
        status = query.get("status")
        ids: Sequence[SubmissionId] = (
            self._state.submission_ids
            if status is None
            else self._state.submission_ids_by_status[status]
        )
        values: list[SubmissionRecord] = []
        for index in _scan_indexes(ids, start.order, start.after):
            if len(values) > limit:
                break
            value = self._state.submissions[ids[index]]
            if "conversationId" in query and value["conversationId"] != query["conversationId"]:
                continue
            values.append(value)
        return _page(values, limit, start.order)

    async def submission_by_request(
        self,
        conversation_id: ConversationId,
        request_id: str,
        context: Context,
    ) -> SubmissionRecord | None:
        self._assert_open()
        id = self._state.submission_ids_by_request.get(conversation_id, {}).get(request_id)
        if id is None:
            return None
        return cast(SubmissionRecord, copy_json(self._state.submissions[id]))

    async def find_document(
        self,
        address: DocumentAddress,
        at: DocumentPoint,
        context: Context,
    ) -> DocumentRecord | None:
        self._assert_open()
        index = self._state.document_addresses.get(_address_key(address))
        if at == "current":
            if index is None or index.current_id is None:
                return None
            return cast(DocumentRecord, copy_json(self._state.documents[index.current_id].record))
        ids = index.ids if index is not None else []
        for id in ids:
            record = self._state.documents[id].record
            if _is_alive_at(record, at):
                return cast(DocumentRecord, copy_json(record))
        return None

    async def document(self, id: DocumentId, at: DocumentPoint, context: Context) -> StoredDocument | None:
        self._assert_open()
        stored = self._materialize_document(id, at)
        if stored is None:
            return None
        return {
            "record": cast(DocumentRecord, copy_json(stored["record"])),
            "version": stored["version"],
            "value": cast(JsonObject, copy_json(stored["value"])),
            "deltasSinceBase": stored["deltasSinceBase"],
        }

    async def scan_documents(
        self,
        query: DocumentQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[DocumentRecord, Cursor]:
        self._assert_open()
        ids = self._state.document_ids_by_scope.get(_scope_key(query["scope"]), [])
        after = _cursor_id(cursor)
        start = 0 if after is None else _upper_bound(ids, after)
        values: list[DocumentRecord] = []
        for index in range(start, len(ids)):
            if len(values) > limit:
                break
            record = self._state.documents[ids[index]].record
            if "kind" in query and record["kind"] != query["kind"]:
                continue
            if _is_alive_at(record, query["at"]):
                values.append(record)
        return _page(values, limit, "ascending")

    async def close(self, context: Context) -> None:
        self._closed = True

    # ── Internals ───────────────────────────────────────────────────────────

    def _materialize_document(self, id: DocumentId, at: DocumentPoint) -> StoredDocument | None:
        stored = self._state.documents.get(id)
        if stored is None:
            return None
        if at != "current" and _is_current_only(stored.record):
            raise StorageRequestError(f"Document {id} does not retain historical content")
        if not _is_alive_at(stored.record, at):
            return None
        revisions = (
            stored.revisions
            if at == "current"
            else [revision for revision in stored.revisions if revision.seq <= at]
        )
        base_index = len(revisions) - 1
        while base_index >= 0 and revisions[base_index].content["kind"] != "base":
            base_index -= 1
        if base_index < 0:
            raise ValueError(f"Document {id} is missing a required base")
        base = revisions[base_index].content
        assert base["kind"] == "base"
        version = base["version"]
        batches: list[Sequence[Op]] = []
        for revision in revisions[base_index + 1:]:
            content = revision.content
            if content["kind"] != "delta" or content["version"] != version:
                raise ValueError(f"Document {id} crosses a stored version boundary without a base")
            batches.append(content["ops"])
        value = apply_immutable_batches(cast(JsonValue, base["value"]), batches)
        return {
            "record": stored.record,
            "version": version,
            "value": cast(JsonObject, value),
            "deltasSinceBase": len(revisions) - base_index - 1,
        }

    def _visible_entries(
        self,
        conversation_id: ConversationId,
        min_entry_id: int | None = None,
        max_entry_id: int | None = None,
    ) -> Iterable[EntryRecord]:
        if conversation_id not in self._state.conversations:
            raise StorageRequestError(f"Unknown conversation: {conversation_id}")
        minimum = -(2**63) if min_entry_id is None else min_entry_id
        upper = 2**63 - 1 if max_entry_id is None else max_entry_id
        current_id = conversation_id
        while True:
            ids = self._state.entry_ids.get(current_id, [])
            for index in range(_upper_bound(ids, upper) - 1, -1, -1):
                id = ids[index]
                if id < minimum:
                    break
                yield self._state.entries[id]
            conversation = self._state.conversations[current_id]
            parent = conversation.get("parent")
            if parent is None:
                break
            upper = min(upper, parent["at"])
            if upper < minimum:
                break
            current_id = parent["conversationId"]

    def _visible_entries_ascending(
        self,
        conversation_id: ConversationId,
        min_entry_id: int | None = None,
        max_entry_id: int | None = None,
    ) -> Iterable[EntryRecord]:
        if conversation_id not in self._state.conversations:
            raise StorageRequestError(f"Unknown conversation: {conversation_id}")
        minimum = -(2**63) if min_entry_id is None else min_entry_id
        upper = 2**63 - 1 if max_entry_id is None else max_entry_id
        segments: list[tuple[ConversationId, int]] = []
        current_id = conversation_id
        while True:
            segments.append((current_id, upper))
            conversation = self._state.conversations[current_id]
            parent = conversation.get("parent")
            if parent is None:
                break
            upper = min(upper, parent["at"])
            if upper < minimum:
                break
            current_id = parent["conversationId"]
        for segment_id, segment_upper in reversed(segments):
            ids = self._state.entry_ids.get(segment_id, [])
            for index in range(_lower_bound(ids, minimum), len(ids)):
                id = ids[index]
                if id > segment_upper:
                    break
                yield self._state.entries[id]

    def _assert_open(self) -> None:
        if self._closed:
            raise RuntimeError("MemoryStorage is closed")


__all__ = ["MemoryStorage", "PreparedMemoryCommit"]
