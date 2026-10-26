"""One atomic Session transaction.

The callback sees table reads only before its first table write, and every document acquisition is
staged. Settlement validates ownership and terminal-task retirement before storage admission;
adoption swaps committed document values in memory only after storage succeeds.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Literal, Protocol, cast

from ..context import Context
from ..delta import Op
from ..documents import (
    AnyDocToken,
    DocDefinition,
    address_id,
    check_record_scope,
    check_record_version,
    document_create,
    materialize_document_value,
    resolve_address,
)
from ..entries import Entry
from ..errors import ReadAfterWrite
from ..ids import (
    ROOT_CONVERSATION_ID,
    ConversationId,
    DocumentId,
    EntryId,
    Seq,
    SubmissionId,
    TaskId,
)
from ..json import JsonObject, JsonValue, copy_json
from ..types import (
    ConversationOwnership,
    ConversationQuery,
    ConversationRecord,
    Cursor,
    DocumentAddress,
    DocumentCommitChange,
    DocumentCopySource,
    DocumentCreate,
    DocumentRecord,
    EntryDraft,
    EntryQuery,
    EntryRecord,
    EntryWithHead,
    Page,
    Storage,
    StorageWrite,
    SubmissionCreate,
    SubmissionRecord,
    SubmissionSettlement,
    TaskOptions,
    TaskQuery,
    TaskRecord,
    entry_record,
)
from .drafts import DocumentDraft, PreparedDraft
from .forks import prepare_fork_document_copies

AnyTaskRecord = TaskRecord
_INTERNAL_SCAN_PAGE_SIZE = 256


@dataclass(slots=True)
class LoadedDocument:
    """One committed document incarnation owned by the Session tracker cache."""

    address_id: str
    record: DocumentRecord
    stored_version: int
    value_version: int
    deltas_since_base: int
    value: JsonObject


class TransactionHost(Protocol):
    """Session services used by a transaction while it holds the mutation line."""

    storage: Storage

    def now(self) -> int: ...

    def cached(self, address_id: str) -> LoadedDocument | None: ...

    async def load(
        self,
        definition: DocDefinition,
        address_id: str,
        address: DocumentAddress,
        context: Context,
    ) -> LoadedDocument | None: ...

    def install(self, document: LoadedDocument) -> None: ...

    def evict(self, address_id: str, record_id: DocumentId) -> None: ...

    async def conversation_created(self, tx: Transaction, record: ConversationRecord) -> None: ...


@dataclass(slots=True)
class TransactionTask:
    committed_read: asyncio.Future[AnyTaskRecord | None] | None = None
    write: tuple[Literal["create", "replace"], AnyTaskRecord] | None = None
    publication_conversation_id: int | None = None


@dataclass(frozen=True, slots=True)
class TransactionScope:
    conversation_id: ConversationId | None = None
    task_id: TaskId | None = None


@dataclass(slots=True)
class _LoadedTarget:
    document: LoadedDocument


@dataclass(slots=True)
class _CreatedTarget:
    record: DocumentCreate
    version: int
    value: JsonObject


@dataclass(slots=True)
class _ForkCopyTarget:
    record: DocumentCreate
    source: DocumentCopySource


@dataclass(slots=True)
class _RetireOnlyTarget:
    record: DocumentRecord


DocumentTarget = _LoadedTarget | _CreatedTarget | _ForkCopyTarget | _RetireOnlyTarget


@dataclass(slots=True)
class DocumentEntry:
    address_id: str
    address: DocumentAddress
    definition: DocDefinition | None = None
    draft: DocumentDraft | None = None
    acquisition: asyncio.Future[DocumentDraft] | None = None
    target: DocumentTarget | None = None
    retire_on_commit: bool = False
    prepared: PreparedDraft | None = None


@dataclass(slots=True)
class _PlanChange:
    loaded: LoadedDocument | None
    definition: DocDefinition | None
    version: int
    value: JsonObject
    ops: list[Op]


@dataclass(slots=True)
class DocumentPlan:
    address_id: str
    record: DocumentCreate | DocumentRecord
    retire: bool = False
    content: StorageWrite | None = None
    change: _PlanChange | None = None
    conversation_id: int | None = None


SubmissionChange = SubmissionSettlement | dict[str, Any]


def _apply_submission_change(current: SubmissionRecord, change: SubmissionChange) -> SubmissionRecord:
    if current["status"] in ("done", "unanswered"):
        return current
    if change.get("status") == "placed":
        if current["status"] != "queued":
            raise ValueError(f"Submission {current['id']} is not queued")
        status = "placed" if current["type"] == "input" else "done"
        return cast(SubmissionRecord, {**current, "status": status, "entry": change["entry"]})
    if change.get("status") == "done" and current["status"] != "placed":
        raise ValueError(f"Submission {current['id']} is not a placed input")
    return cast(SubmissionRecord, {**current, **change})


class Transaction:
    """Transaction for one Session commit callback."""

    def __init__(
        self,
        host: TransactionHost,
        context: Context,
        scope: TransactionScope | None = None,
    ) -> None:
        self._host = host
        self._context = context
        self._scope = scope or TransactionScope()
        self._pending: set[asyncio.Future[Any]] = set()
        self._sealed = False
        self._has_table_write = False
        self._writes: list[StorageWrite] = []
        self._created_conversation_ids: set[int] = set()
        self._fork_source_conversation_ids: set[int] = set()
        self._fork_source_document_ids: set[int] = set()
        self._tasks: dict[TaskId, TransactionTask] = {}
        self._submissions: dict[SubmissionId, SubmissionRecord] = {}
        self._submission_changes: list[tuple[SubmissionId, SubmissionChange]] = []
        self._plans: list[DocumentPlan] = []
        self._documents: list[DocumentEntry] = []
        self._latest_document_by_address: dict[str, DocumentEntry] = {}

    # ── Table reads ────────────────────────────────────────────────────────

    async def conversation(self, id: ConversationId) -> ConversationRecord | None:
        return await self._read("conversation", lambda: self._host.storage.conversation(id, self._context))

    async def entry(self, first: EntryId | Entry[Any], second: EntryId | None = None) -> EntryRecord | None:
        kind = None if isinstance(first, int) else first.kind
        id = first if isinstance(first, int) else second
        assert id is not None

        async def read() -> EntryRecord | None:
            stored = await self._host.storage.entry(id, self._context)
            if stored is None:
                return None
            if kind is not None and stored["entry"].get("kind") != kind:
                return None
            return stored["entry"]

        return await self._read("entry", read)

    async def task(self, id: TaskId) -> AnyTaskRecord | None:
        return await self._read("task", lambda: self._committed_task(id))

    async def scan_conversations(
        self,
        query: ConversationQuery,
        limit: int,
        cursor: Cursor | None = None,
    ) -> Page[ConversationRecord, Cursor]:
        return await self._read(
            "scan_conversations",
            lambda: self._host.storage.scan_conversations(query, limit, cursor, self._context),
        )

    async def scan_entries(
        self,
        query: EntryQuery,
        limit: int,
        cursor: Cursor | None = None,
    ) -> Page[EntryRecord, Cursor]:
        return await self._read(
            "scan_entries",
            lambda: self._host.storage.scan_entries(query, limit, cursor, self._context),
        )

    async def latest_head_marker(self, conversation_id: ConversationId) -> EntryWithHead | None:
        return await self._read(
            "latest_head_marker",
            lambda: self._host.storage.find_latest_head_marker(conversation_id, None, self._context),
        )

    async def scan_tasks(
        self,
        query: TaskQuery,
        limit: int,
        cursor: Cursor | None = None,
    ) -> Page[TaskRecord, Cursor]:
        return await self._read(
            "scan_tasks",
            lambda: self._host.storage.scan_tasks(query, limit, cursor, self._context),
        )

    async def submission(self, id: SubmissionId) -> SubmissionRecord | None:
        return await self._read("submission", lambda: self._host.storage.submission(id, self._context))

    async def submission_by_request(
        self,
        conversation_id: ConversationId,
        request_id: str,
    ) -> SubmissionRecord | None:
        return await self._read(
            "submission_by_request",
            lambda: self._host.storage.submission_by_request(conversation_id, request_id, self._context),
        )

    # ── Table writes ───────────────────────────────────────────────────────

    async def create_conversation(self, options: dict[str, Any]) -> ConversationRecord:
        ownership = cast(ConversationOwnership, options["ownership"])
        return await self._write(lambda: self._stage_conversation(None, ownership))

    async def create_root_conversation(self) -> ConversationRecord:
        return await self._write(
            lambda: self._stage_conversation(None, {"kind": "ownerless"}, ROOT_CONVERSATION_ID)
        )

    async def fork_conversation(
        self,
        parent_conversation_id: ConversationId,
        at: EntryId,
        options: dict[str, Any],
    ) -> ConversationRecord:
        ownership = cast(ConversationOwnership, options["ownership"])
        return await self._write(
            lambda: self._stage_conversation(
                {"conversationId": parent_conversation_id, "at": at},
                ownership,
            )
        )

    async def _stage_conversation(
        self,
        parent: dict[str, Any] | None,
        ownership: ConversationOwnership,
        reserved_id: ConversationId | None = None,
    ) -> ConversationRecord:
        owner_task_id = ownership["taskId"] if ownership["kind"] == "task" else None
        id = (
            reserved_id
            if reserved_id is not None
            else cast(ConversationId, await self._host.storage.mint_id())
        )
        self._assert_open()
        owner: dict[str, Any] | None = None
        if owner_task_id is not None:
            task = await self._current_task(owner_task_id)
            self._assert_open()
            if task is None:
                raise ValueError(f"Conversation owner task {owner_task_id} does not exist")
            owner = {"conversationId": task["conversationId"], "taskId": owner_task_id}
        record: dict[str, Any] = {"id": id}
        if parent is not None:
            record["parent"] = parent
        if owner is not None:
            record["owner"] = owner
        copies = (
            []
            if parent is None
            else await prepare_fork_document_copies(
                self._host.storage,
                parent["conversationId"],
                parent["at"],
                id,
                self._context,
            )
        )
        self._assert_open()
        for copy in copies:
            self._fork_source_document_ids.add(copy.source["id"])
            entry = DocumentEntry(
                address_id=address_id(copy.record),
                address={"kind": copy.record["kind"], "scope": copy.record["scope"]},
                target=_ForkCopyTarget(copy.record, copy.source),
            )
            if "key" in copy.record:
                entry.address["key"] = copy.record["key"]
            self._documents.append(entry)
            self._latest_document_by_address[entry.address_id] = entry
        if parent is not None:
            self._fork_source_conversation_ids.add(parent["conversationId"])
        self._created_conversation_ids.add(id)
        self._writes.append(cast(StorageWrite, {"type": "conversation", "value": record}))
        await self._host.conversation_created(self, cast(ConversationRecord, record))
        self._assert_open()
        return cast(ConversationRecord, record)

    async def append_entry(
        self,
        first: ConversationId | Entry[Any],
        second: ConversationId | EntryDraft,
        third: EntryDraft | None = None,
    ) -> EntryRecord:
        if isinstance(first, int):
            conversation_id = first
            value = cast(EntryDraft, second)
        else:
            conversation_id = cast(ConversationId, second)
            value = cast(EntryDraft, {**(third or {}), "kind": first.kind})
        return await self._write(lambda: self._append_entry(conversation_id, value))

    async def _append_entry(self, conversation_id: ConversationId, value: EntryDraft) -> EntryRecord:
        await self._require_conversation(conversation_id)
        self._assert_open()
        id = cast(EntryId, await self._host.storage.mint_id())
        self._assert_open()
        record = entry_record(id, conversation_id, value, by_task_id=self._scope.task_id)
        self._writes.append({"type": "entry", "value": record})
        return record

    async def create_task(self, task: Any, input: JsonValue, options: TaskOptions) -> TaskId:
        return await self._write(lambda: self._create_task(task, input, options))

    async def _create_task(self, task: Any, input: JsonValue, options: TaskOptions) -> TaskId:
        ownership = options["ownership"]
        owner: AnyTaskRecord | None = None
        if ownership["kind"] == "task":
            owner = await self._current_task(ownership["taskId"])
            self._assert_open()
            if owner is None:
                raise ValueError(f"Task owner {ownership['taskId']} does not exist")
            if options.get("background") is True:
                raise TypeError("A child task cannot be background")
            if (
                options.get("conversationId") is not None
                and options["conversationId"] != owner["conversationId"]
            ):
                raise ValueError(
                    f"A child task lives in its owner's conversation {owner['conversationId']}"
                )
        conversation_id = (
            (owner["conversationId"] if owner is not None else None)
            or options.get("conversationId")
            or self._scope.conversation_id
        )
        if conversation_id is None:
            raise TypeError("Tx.createTask() requires options.conversationId")
        await self._require_conversation(conversation_id)
        self._assert_open()
        definition = task.definition
        checkpoint = copy_json(definition.initial(input))
        id = cast(TaskId, await self._host.storage.mint_id())
        self._assert_open()
        record: dict[str, Any] = {
            "id": id,
            "conversationId": conversation_id,
            "kind": definition.name,
            "version": definition.version,
            "input": copy_json(input),
            "background": options.get("background", False),
            "abortRequested": False,
            "state": {"status": "pending", "checkpoint": checkpoint},
        }
        if owner is not None:
            record["owner"] = owner["id"]
        if options.get("abandonOnRestart") is True:
            record["abandonOnRestart"] = True
        self._task_entry(id).write = ("create", cast(AnyTaskRecord, copy_json(record)))
        return id

    async def create_submission(self, create: SubmissionCreate) -> SubmissionRecord:
        return await self._write(lambda: self._create_submission(create))

    async def _create_submission(self, create: SubmissionCreate) -> SubmissionRecord:
        await self._require_conversation(create["conversationId"])
        self._assert_open()
        id = cast(SubmissionId, await self._host.storage.mint_id())
        self._assert_open()
        record = cast(SubmissionRecord, copy_json({**create, "id": id}))
        self._submissions[id] = record
        return record

    def settle_submission(self, id: SubmissionId, settlement: SubmissionSettlement) -> None:
        self._assert_open()
        self._has_table_write = True
        self._submission_changes.append((id, cast(SubmissionChange, copy_json(settlement))))

    def place_submission(self, id: SubmissionId, entry: EntryId) -> None:
        self._assert_open()
        self._has_table_write = True
        self._submission_changes.append((id, {"status": "placed", "entry": entry}))

    def set_task(self, value: AnyTaskRecord) -> None:
        self._assert_open()
        self._has_table_write = True
        task = self._task_entry(value["id"])
        candidate = task.write[1] if task.write is not None else None
        if candidate is not None and candidate["state"]["status"] == "terminal":
            raise ValueError(f"Task {value['id']} already has a terminal candidate")
        if candidate is not None and candidate["conversationId"] != value["conversationId"]:
            raise ValueError(f"Task {value['id']} cannot change conversations")
        kind = "create" if task.write is not None and task.write[0] == "create" else "replace"
        task.write = (kind, cast(AnyTaskRecord, copy_json(self._stamp_times(value, candidate))))

    def staged_tasks(self) -> list[AnyTaskRecord]:
        return [task.write[1] for task in self._tasks.values() if task.write is not None]

    def staged_conversations(self) -> list[ConversationRecord]:
        return [
            write["value"]
            for write in self._writes
            if write["type"] == "conversation"
        ]

    # ── Documents ──────────────────────────────────────────────────────────

    async def doc(self, token: AnyDocToken, *args: Any) -> DocumentDraft:
        self._assert_open()
        definition = token.definition
        resolved = resolve_address(definition, args)
        self._assert_task_documents_open(resolved.address)
        latest = self._latest_document_by_address.get(resolved.id)
        if latest is not None and not latest.retire_on_commit:
            if latest.draft is not None:
                return latest.draft
            if latest.acquisition is not None:
                return await latest.acquisition
            if isinstance(latest.target, _ForkCopyTarget):
                latest.acquisition = asyncio.ensure_future(
                    self._track(self._acquire_fork_copy(latest, definition, latest.target))
                )
                return await latest.acquisition
        seed = copy_json(args[resolved.next_argument]) if definition.family else None
        entry = DocumentEntry(address_id=resolved.id, address=resolved.address, definition=definition)
        self._documents.append(entry)
        self._latest_document_by_address[entry.address_id] = entry
        entry.acquisition = asyncio.ensure_future(
            self._track(self._acquire(entry, seed, latest is not None and latest.retire_on_commit))
        )
        return await entry.acquisition

    async def retrieve_doc_draft(self, entry: DocumentEntry) -> DocumentDraft:
        if entry.draft is not None:
            return entry.draft
        if entry.acquisition is not None:
            return await entry.acquisition
        target = entry.target
        if isinstance(target, _CreatedTarget):
            entry.draft = DocumentDraft(target.value)
            return entry.draft
        if isinstance(target, _LoadedTarget):
            entry.draft = DocumentDraft(target.document.value)
            return entry.draft
        raise RuntimeError("Document target has no draft")

    async def retire_doc(self, token: AnyDocToken, *args: Any) -> None:
        self._assert_open()
        definition = token.definition
        resolved = resolve_address(definition, args)
        latest = self._latest_document_by_address.get(resolved.id)
        if latest is not None and latest.retire_on_commit:
            return
        if latest is not None and isinstance(latest.target, _ForkCopyTarget):
            latest.retire_on_commit = True
            return
        if latest is not None and (latest.draft is not None or latest.acquisition is not None):
            latest.retire_on_commit = True
            await self.retrieve_doc_draft(latest)
            return
        entry = DocumentEntry(
            address_id=resolved.id,
            address=resolved.address,
            definition=definition,
            retire_on_commit=True,
        )
        self._documents.append(entry)
        self._latest_document_by_address[entry.address_id] = entry
        await self._track(self._find_retirement(entry))

    async def _acquire(
        self,
        entry: DocumentEntry,
        seed: JsonValue | None,
        skip_load: bool,
    ) -> DocumentDraft:
        definition = entry.definition
        assert definition is not None
        loaded = (
            None
            if skip_load
            else await self._host.load(definition, entry.address_id, entry.address, self._context)
        )
        self._assert_open()
        if loaded is not None:
            check_record_scope(definition, loaded.record)
            check_record_version(definition, loaded.record, loaded.stored_version)
            entry.target = _LoadedTarget(loaded)
            entry.draft = DocumentDraft(loaded.value)
            return entry.draft
        scope = entry.address["scope"]
        if scope["kind"] == "conversation":
            await self._require_conversation(scope["conversationId"])
        elif scope["kind"] == "task":
            task = await self._current_task(scope["taskId"])
            if task is None:
                raise ValueError(f"Task {scope['taskId']} does not exist")
            if task["state"]["status"] == "terminal":
                raise ValueError(f"Task {scope['taskId']} is terminal")
        self._assert_open()
        value = copy_json(
            definition.initial(seed) if definition.family else definition.initial()
        )
        if not isinstance(value, dict):
            raise TypeError(f"Document {definition.kind} initial() must return an object")
        id = cast(DocumentId, await self._host.storage.mint_id())
        self._assert_open()
        record = document_create(definition, entry.address, id)
        entry.target = _CreatedTarget(record, definition.version, value)
        entry.draft = DocumentDraft(value)
        return entry.draft

    async def _acquire_fork_copy(
        self,
        entry: DocumentEntry,
        definition: DocDefinition,
        target: _ForkCopyTarget,
    ) -> DocumentDraft:
        stored = await self._host.storage.document(target.source["id"], target.source["at"], self._context)
        self._assert_open()
        if stored is None:
            raise ValueError(f"Fork source document {target.source['id']} cannot be read")
        record = stored["record"]
        if (
            record["scope"]["kind"] != "conversation"
            or record["kind"] != target.record["kind"]
            or record.get("key") != target.record.get("key")
            or record.get("history") != target.record.get("history")
            or record.get("fork") != target.record.get("fork")
        ):
            raise ValueError(f"Fork source document {target.source['id']} does not match the copied record")
        value = materialize_document_value(definition, target.record, stored["version"], stored["value"])
        entry.definition = definition
        entry.target = _CreatedTarget(target.record, definition.version, value)
        entry.draft = DocumentDraft(value)
        return entry.draft

    async def _find_retirement(self, entry: DocumentEntry) -> None:
        cached = self._host.cached(entry.address_id)
        record = (
            cached.record
            if cached is not None
            else await self._host.storage.find_document(entry.address, "current", self._context)
        )
        self._assert_open()
        if record is None:
            return
        assert entry.definition is not None
        check_record_scope(entry.definition, record)
        entry.target = _RetireOnlyTarget(record)

    # ── Settlement ─────────────────────────────────────────────────────────

    async def settle_failure(self) -> None:
        self._sealed = True
        for document in self._documents:
            if document.draft is not None:
                document.draft._seal()
        await asyncio.gather(*list(self._pending), return_exceptions=True)

    async def settle_success(self) -> list[StorageWrite]:
        self._sealed = True
        if self._pending:
            for document in self._documents:
                if document.draft is not None:
                    document.draft._seal()
            await asyncio.gather(*list(self._pending), return_exceptions=True)
            raise RuntimeError("Session commit callback settled before its pending Tx operations")
        try:
            for document in self._documents:
                if document.draft is not None:
                    document.prepared = document.draft._prepare()
            return await self._assemble()
        except Exception:
            for document in self._documents:
                if document.draft is not None:
                    document.draft._seal()
            raise

    def discard(self) -> None:
        for document in self._documents:
            if document.draft is not None:
                document.draft._seal()

    def adopt(self, seq: Seq) -> list[DocumentCommitChange]:
        publications: list[DocumentCommitChange] = []
        for plan in self._plans:
            committed = "createdAt" in plan.record
            record: DocumentRecord = (
                cast(DocumentRecord, plan.record)
                if committed
                else cast(DocumentRecord, {**plan.record, "createdAt": seq})
            )
            if plan.retire:
                record = cast(DocumentRecord, {**record, "retiredAt": seq})
            change = plan.change
            if plan.change is not None:
                loaded = plan.change.loaded
                if loaded is None:
                    if not plan.retire:
                        self._host.install(
                            LoadedDocument(
                                address_id=plan.address_id,
                                record=record,
                                stored_version=plan.change.version,
                                value_version=plan.change.version,
                                deltas_since_base=0,
                                value=plan.change.value,
                            )
                        )
                else:
                    if plan.content is not None:
                        loaded.value = plan.change.value
                        loaded.stored_version = plan.change.version
                        if plan.content["type"] == "document.change":
                            content = plan.content["content"]
                            if content["kind"] == "base":
                                loaded.deltas_since_base = 0
                            else:
                                loaded.deltas_since_base += 1
            if plan.retire:
                if committed:
                    self._host.evict(plan.address_id, record["id"])
                publications.append(
                    {
                        "type": "document",
                        "record": record,
                        "conversationId": plan.conversation_id,
                        "version": None,
                        "value": None,
                        "ops": [],
                    }
                )
            elif plan.content is not None and plan.content["type"] == "document.copy":
                publications.append(
                    {
                        "type": "document.copy",
                        "record": record,
                        "conversationId": plan.conversation_id,
                        "source": plan.content["source"],
                    }
                )
            elif change is not None and (change.loaded is None or plan.content is not None):
                publications.append(
                    {
                        "type": "document",
                        "record": record,
                        "conversationId": plan.conversation_id,
                        "version": change.version,
                        "value": change.value,
                        "ops": [] if change.loaded is None else change.ops,
                    }
                )
        return publications

    async def _assemble(self) -> list[StorageWrite]:
        storage = self._host.storage
        for document in self._documents:
            plan = self._plan_document(document)
            if plan is not None:
                self._plans.append(plan)
        self._reject_fork_source_writes(self._plans)
        await self._validate_owners()
        for id, task in self._tasks.items():
            if task.write is None or task.write[0] != "replace":
                continue
            committed = await self._committed_task(id)
            if committed is None:
                raise ValueError(f"Task {id} does not exist")
            if committed["state"]["status"] == "terminal":
                raise ValueError(f"Task {id} is already terminal")
            if committed["conversationId"] != task.write[1]["conversationId"]:
                raise ValueError(f"Task {id} cannot change conversations")

        terminal_task_ids: set[int] = set()
        for task in self._tasks.values():
            if task.write is not None and task.write[1]["state"]["status"] == "terminal":
                terminal_task_ids.add(task.write[1]["id"])
        if terminal_task_ids:
            retiring: set[int] = set()
            for plan in self._plans:
                scope = plan.record["scope"]
                if scope["kind"] != "task" or scope["taskId"] not in terminal_task_ids:
                    continue
                plan.retire = True
                retiring.add(plan.record["id"])
            for task_id in terminal_task_ids:
                candidate = self._tasks.get(task_id)
                if candidate is not None and candidate.write is not None and candidate.write[0] == "create":
                    continue
                cursor: Cursor | None = None
                while True:
                    page = await storage.scan_documents(
                        {"scope": {"kind": "task", "taskId": task_id}, "at": "current"},
                        _INTERNAL_SCAN_PAGE_SIZE,
                        cursor,
                        self._context,
                    )
                    for record in page["items"]:
                        if record["id"] in retiring:
                            continue
                        self._plans.append(
                            DocumentPlan(address_id=address_id(record), record=record, retire=True)
                        )
                        retiring.add(record["id"])
                    cursor = page.get("next")
                    if cursor is None:
                        break

        for plan in self._plans:
            if not self._publishes(plan):
                continue
            scope = plan.record["scope"]
            if scope["kind"] == "conversation":
                plan.conversation_id = scope["conversationId"]
                continue
            if scope["kind"] != "task":
                continue
            task = self._task_entry(scope["taskId"])
            if task.publication_conversation_id is None:
                current = await self._current_task(scope["taskId"])
                if current is not None:
                    task.publication_conversation_id = current["conversationId"]
            plan.conversation_id = task.publication_conversation_id

        for id, change in self._submission_changes:
            current = self._submissions.get(id)
            if current is None:
                current = await storage.submission(id, self._context)
            if current is None:
                raise ValueError(f"Submission {id} does not exist")
            next_record = _apply_submission_change(current, change)
            if next_record is not current:
                self._submissions[id] = next_record

        writes = self._writes
        for value in self._submissions.values():
            writes.append({"type": "submission", "value": value})
        for task in self._tasks.values():
            if task.write is not None:
                writes.append({"type": "task", "value": task.write[1]})
        for plan in self._plans:
            change = plan.change
            if (
                plan.content is not None
                and plan.content["type"] == "document.change"
                and plan.content["content"]["kind"] == "delta"
                and change is not None
                and change.loaded is not None
                and change.definition is not None
            ):
                info = {"deltasSinceBase": change.loaded.deltas_since_base}
                predicate = change.definition.checkpoint_when
                assert predicate is not None, "every definition supplies checkpoint_when"
                if predicate(cast(JsonObject, change.value), change.ops, info):
                    plan.content = cast(
                        StorageWrite,
                        {
                            **plan.content,
                            "content": {
                                "version": change.version,
                                "kind": "base",
                                "value": change.value,
                            },
                        },
                    )
            if plan.content is not None:
                writes.append(plan.content)
            if plan.retire:
                writes.append({"type": "document.retire", "id": plan.record["id"]})
        return writes

    # ── Helpers ────────────────────────────────────────────────────────────

    async def _validate_owners(self) -> None:
        owners: list[tuple[str, int]] = []
        for write in self._writes:
            if write["type"] != "conversation":
                continue
            owner = write["value"].get("owner")
            if owner is not None:
                owners.append(("Conversation owner task", owner["taskId"]))
        for task in self._tasks.values():
            if task.write is None or task.write[0] != "create":
                continue
            owner = task.write[1].get("owner")
            if owner is not None:
                owners.append(("Task owner", owner))
        for what, task_id in owners:
            task = await self._current_task(cast(TaskId, task_id))
            if task is None:
                raise ValueError(f"{what} {task_id} does not exist")
            status = task["state"]["status"]
            if status in ("terminal", "completing"):
                raise ValueError(f"{what} {task_id} is {status}")
            if task["abortRequested"]:
                raise ValueError(f"{what} {task_id} is abort-marked")

    def _reject_fork_source_writes(self, plans: list[DocumentPlan]) -> None:
        for plan in plans:
            if plan.content is None and not plan.retire:
                continue
            record = plan.record
            if record["id"] in self._fork_source_document_ids:
                raise ValueError(
                    f"Cannot change fork source document {record['id']} in the fork transaction"
                )
            scope = record["scope"]
            if (
                scope["kind"] == "conversation"
                and scope["conversationId"] in self._fork_source_conversation_ids
                and record["scope"]["kind"] == "conversation"
                and record.get("fork") == "current"
            ):
                raise ValueError(
                    f"Cannot fork conversation {scope['conversationId']} "
                    "while changing its current-policy documents"
                )

    def _plan_document(self, document: DocumentEntry) -> DocumentPlan | None:
        target = document.target
        if target is None:
            return None
        if isinstance(target, _RetireOnlyTarget):
            return DocumentPlan(
                address_id=document.address_id,
                record=target.record,
                retire=document.retire_on_commit,
            )
        if isinstance(target, _ForkCopyTarget):
            return DocumentPlan(
                address_id=document.address_id,
                record=target.record,
                retire=document.retire_on_commit,
                content=cast(
                    StorageWrite,
                    {"type": "document.copy", "record": target.record, "source": target.source},
                ),
            )
        prepared = document.prepared
        assert prepared is not None
        if isinstance(target, _CreatedTarget):
            return DocumentPlan(
                address_id=document.address_id,
                record=target.record,
                retire=document.retire_on_commit,
                content=cast(
                    StorageWrite,
                    {
                        "type": "document.create",
                        "record": target.record,
                        "content": {"version": target.version, "kind": "base", "value": prepared.value},
                    },
                ),
                change=_PlanChange(
                    loaded=None,
                    definition=document.definition,
                    version=target.version,
                    value=prepared.value,
                    ops=[],
                ),
            )
        loaded = target.document
        definition = document.definition
        assert definition is not None
        ops = prepared.ops
        value = prepared.value
        if loaded.stored_version < definition.version:
            content = cast(
                StorageWrite,
                {
                    "type": "document.change",
                    "id": loaded.record["id"],
                    "content": {"version": definition.version, "kind": "base", "value": value},
                },
            )
        elif ops:
            content = cast(
                StorageWrite,
                {
                    "type": "document.change",
                    "id": loaded.record["id"],
                    "content": {"version": definition.version, "kind": "delta", "ops": ops},
                },
            )
        else:
            content = None
        return DocumentPlan(
            address_id=document.address_id,
            record=loaded.record,
            retire=document.retire_on_commit,
            content=content,
            change=_PlanChange(
                loaded=loaded,
                definition=definition,
                version=definition.version,
                value=value,
                ops=ops,
            ),
        )

    @staticmethod
    def _publishes(plan: DocumentPlan) -> bool:
        return plan.retire or plan.change is None or plan.change.loaded is None or plan.content is not None

    def _assert_open(self) -> None:
        if self._sealed:
            raise RuntimeError("Transaction has settled")

    def _assert_task_documents_open(self, address: DocumentAddress) -> None:
        scope = address["scope"]
        if scope["kind"] != "task":
            return
        task = self._tasks.get(scope["taskId"])
        if task is not None and task.write is not None and task.write[1]["state"]["status"] == "terminal":
            raise ValueError(f"Task {scope['taskId']} is terminal")

    def _track(self, awaitable: Any) -> asyncio.Future[Any]:
        task = asyncio.ensure_future(awaitable)
        self._pending.add(task)

        def settle(_: Any) -> None:
            self._pending.discard(task)

        task.add_done_callback(settle)
        return task

    async def _read(self, method: str, read: Any) -> Any:
        self._assert_open()
        if self._has_table_write:
            raise ReadAfterWrite(method)
        return await self._track(read())

    async def _write(self, write: Any) -> Any:
        self._assert_open()
        self._has_table_write = True
        return await self._track(write())

    async def _require_conversation(self, id: ConversationId) -> None:
        if id in self._created_conversation_ids:
            return
        if await self._host.storage.conversation(id, self._context) is None:
            raise ValueError(f"Conversation {id} does not exist")

    def _stamp_times(self, value: AnyTaskRecord, candidate: AnyTaskRecord | None) -> AnyTaskRecord:
        status = value["state"]["status"]

        def stamp_enabled(enabled: bool) -> int | None:
            return self._host.now() if enabled else None

        def first_set(*values: int | None) -> int | None:
            return next((item for item in values if item is not None), None)

        started_at = first_set(
            (candidate or {}).get("startedAt"),
            value.get("startedAt"),
            stamp_enabled(status == "running"),
        )
        ended_at = first_set(
            (candidate or {}).get("endedAt"),
            value.get("endedAt"),
            stamp_enabled(status == "terminal"),
        )
        if started_at == value.get("startedAt") and ended_at == value.get("endedAt"):
            return value
        result = dict(value)
        if started_at is None:
            result.pop("startedAt", None)
        else:
            result["startedAt"] = started_at
        if ended_at is None:
            result.pop("endedAt", None)
        else:
            result["endedAt"] = ended_at
        return cast(AnyTaskRecord, result)

    def _task_entry(self, id: TaskId) -> TransactionTask:
        return self._tasks.setdefault(id, TransactionTask())

    async def _current_task(self, id: TaskId) -> AnyTaskRecord | None:
        task = self._tasks.get(id)
        if task is not None and task.write is not None:
            return task.write[1]
        return await self._committed_task(id)

    async def _committed_task(self, id: TaskId) -> AnyTaskRecord | None:
        task = self._task_entry(id)
        if task.committed_read is None:
            task.committed_read = asyncio.ensure_future(
                self._host.storage.task(id, self._context)
            )
        return await task.committed_read


__all__ = [
    "DocumentEntry",
    "DocumentPlan",
    "LoadedDocument",
    "Transaction",
    "TransactionHost",
    "TransactionScope",
]
