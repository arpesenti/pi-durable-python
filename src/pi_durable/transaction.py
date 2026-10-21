"""Transaction for one Session commit callback.

Every asynchronous operation completes before settlement; settlement revokes every draft,
prepares every change, validates ownership, and assembles one atomic Storage batch. Adoption
after storage succeeds is synchronous and performs no reads.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from .delta import Change, Prepared, Tracker
from .documents import (
    CheckpointInfo,
    DocDefinition,
    DocToken,
    LoadedDocument,
    address_id,
    check_record_scope,
    check_record_version,
    document_create,
    materialize_document_value,
    resolve_address,
)
from .draft import DocumentDraft, begin_draft
from .errors import ReadAfterWrite
from .forks import prepare_fork_document_copies
from .json import copy_json, omit_none
from .types import (
    ROOT_CONVERSATION_ID,
    Cursor,
    Page,
    StorageWrite,
    TaskOptions,
    TransactionScope,
    conversation_record,
    entry_record,
)


class TransactionHost(Protocol):
    storage: Any

    def now(self) -> int: ...

    async def load_document(
        self, definition: DocDefinition, address_id_value: str, address: Mapping[str, Any]
    ) -> LoadedDocument | None: ...

    def cached_document(self, address_id_value: str) -> LoadedDocument | None: ...

    def install_document(self, document: LoadedDocument) -> None: ...

    def evict_document(self, address_id_value: str, record_id: int) -> None: ...

    async def conversation_created(self, tx: Transaction, record: Mapping[str, Any]) -> None: ...


@dataclass
class _DocumentEntry:
    address_id: str
    address: dict[str, Any]
    definition: DocDefinition | None = None
    draft_future: asyncio.Future[DocumentDraft] | None = None
    target: dict[str, Any] | None = None
    change: Change | None = None
    prepared: Prepared | None = None
    retire_on_commit: bool = False


@dataclass
class _DocPlan:
    address_id: str
    record: dict[str, Any]
    created: bool = False
    retire: bool = False
    content: dict[str, Any] | None = None
    change: Change | None = None
    prepared: Prepared | None = None
    loaded: LoadedDocument | None = None
    tracker: Tracker | None = None
    version: int | None = None
    definition: DocDefinition | None = None
    conversation_id: int | None = None


class Transaction:
    """Transaction surface of one Session commit callback."""

    def __init__(self, host: TransactionHost, scope: TransactionScope | None = None) -> None:
        self._host = host
        self._scope = scope or TransactionScope()
        self._sealed = False
        self._has_table_write = False
        self._writes: list[StorageWrite] = []
        self._created_conversation_ids: set[int] = set()
        self._fork_source_conversation_ids: set[int] = set()
        self._fork_source_document_ids: set[int] = set()
        self._tasks: dict[int, dict[str, Any]] = {}
        self._created_task_ids: set[int] = set()
        self._committed_tasks: dict[int, dict[str, Any] | None] = {}
        self._submissions: dict[int, dict[str, Any]] = {}
        self._submission_changes: list[tuple[int, dict[str, Any]]] = []
        self._documents: list[_DocumentEntry] = []
        self._latest_document_by_address: dict[str, _DocumentEntry] = {}
        self._plans: list[_DocPlan] = []

    # ── Table reads ────────────────────────────────────────────────────────

    async def _read(self, method: str, read: Callable[[], Awaitable[Any]]) -> Any:
        self._assert_open()
        if self._has_table_write:
            raise ReadAfterWrite(method)
        return await read()

    async def conversation(self, conversation_id: int) -> dict[str, Any] | None:
        return await self._read("conversation", lambda: self._host.storage.conversation(conversation_id))

    async def entry(self, entry_id: int, token: DocToken | None = None) -> dict[str, Any] | None:
        async def read() -> dict[str, Any] | None:
            entry = await self._host.storage.entry_by_id(entry_id)
            if entry is None:
                return None
            if token is not None and entry["kind"] != token.kind:
                return None
            return copy_json(entry)

        return await self._read("entry", read)

    async def task(self, task_id: int) -> dict[str, Any] | None:
        return await self._read("task", lambda: self._host.storage.task(task_id))

    async def scan_conversations(
        self, query: Mapping[str, Any], limit: int, cursor: Cursor | None = None
    ) -> Page:
        return await self._read(
            "scanConversations", lambda: self._host.storage.scan_conversations(query, limit, cursor)
        )

    async def scan_entries(self, query: Mapping[str, Any], limit: int, cursor: Cursor | None = None) -> Page:
        return await self._read("scanEntries", lambda: self._host.storage.scan_entries(query, limit, cursor))

    async def latest_head_marker(self, conversation_id: int) -> dict[str, Any] | None:
        return await self._read(
            "latestHeadMarker", lambda: self._host.storage.find_latest_head_marker(conversation_id, None)
        )

    async def scan_tasks(self, query: Mapping[str, Any], limit: int, cursor: Cursor | None = None) -> Page:
        return await self._read("scanTasks", lambda: self._host.storage.scan_tasks(query, limit, cursor))

    async def submission_by_request(self, conversation_id: int, request_id: str) -> dict[str, Any] | None:
        return await self._read(
            "submissionByRequest",
            lambda: self._host.storage.submission_by_request(conversation_id, request_id),
        )

    # ── Table writes ───────────────────────────────────────────────────────

    async def create_conversation(self, options: Mapping[str, Any]) -> dict[str, Any]:
        return await self._stage_conversation(None, options["ownership"])

    async def create_root_conversation(self) -> dict[str, Any]:
        return await self._stage_conversation(None, {"kind": "ownerless"}, ROOT_CONVERSATION_ID)

    async def fork_conversation(
        self, parent_conversation_id: int, at: int, options: Mapping[str, Any]
    ) -> dict[str, Any]:
        return await self._stage_conversation(
            {"conversationId": parent_conversation_id, "at": at}, options["ownership"]
        )

    async def _stage_conversation(
        self,
        parent: Mapping[str, Any] | None,
        ownership: Mapping[str, Any],
        reserved_id: int | None = None,
    ) -> dict[str, Any]:
        self._assert_open()
        owner_task_id = ownership.get("taskId") if ownership.get("kind") == "task" else None
        conversation_id = reserved_id if reserved_id is not None else await self._host.storage.mint_id()
        self._assert_open()
        owner = None
        if owner_task_id is not None:
            owner_task = await self._current_task(owner_task_id)
            self._assert_open()
            if owner_task is None:
                raise ValueError(f"Conversation owner task {owner_task_id} does not exist")
            owner = {"conversationId": owner_task["conversationId"], "taskId": owner_task_id}
        copies: list[dict[str, Any]] = []
        if parent is not None:
            copies = await prepare_fork_document_copies(
                self._host.storage, parent["conversationId"], parent["at"], conversation_id
            )
            self._assert_open()
            for copy in copies:
                self._fork_source_document_ids.add(copy["source"]["id"])
                entry = _DocumentEntry(
                    address_id=address_id(copy["record"]),
                    address=copy["record"],
                    target={"kind": "fork-copy", "record": copy["record"], "source": copy["source"]},
                )
                self._documents.append(entry)
                self._latest_document_by_address[entry.address_id] = entry
            self._fork_source_conversation_ids.add(parent["conversationId"])
        record = conversation_record(conversation_id, parent=parent, owner=owner)
        self._created_conversation_ids.add(conversation_id)
        self._writes.append({"type": "conversation", "value": record})
        await self._host.conversation_created(self, record)
        self._assert_open()
        return copy_json(record)

    async def append_entry(
        self, conversation_id: int, value: Mapping[str, Any], token: DocToken | None = None
    ) -> dict[str, Any]:
        """Append one immutable transcript entry.

        ``tx.append_entry(cid, {"kind": ...})`` or ``tx.append_entry(cid, draft, token=Kind)``.
        """
        self._has_table_write = True
        self._assert_open()
        await self._require_conversation(conversation_id)
        self._assert_open()
        entry_id = await self._host.storage.mint_id()
        self._assert_open()
        draft = dict(value)
        if token is not None:
            draft["kind"] = token.kind
        kind = draft.get("kind")
        if not isinstance(kind, str) or not kind:
            raise TypeError("Entry kind must be a non-empty string")
        head = draft.get("head")
        record = entry_record(
            entry_id,
            conversation_id,
            kind,
            model=draft.get("model"),
            data=draft.get("data"),
            head=entry_id if head == "self" else head,
            edits=draft.get("edits"),
            by_task_id=self._scope.task_id,
        )
        self._writes.append({"type": "entry", "value": record})
        return copy_json(record)

    async def create_task(self, task: Any, input: Any, options: TaskOptions) -> int:
        self._has_table_write = True
        self._assert_open()
        ownership = options.ownership
        owner_task = None
        if ownership.get("kind") == "task":
            owner_task = await self._current_task(ownership["taskId"])
            self._assert_open()
            if owner_task is None:
                raise ValueError(f"Task owner {ownership['taskId']} does not exist")
            if options.background:
                raise TypeError("A child task cannot be background")
            if options.conversation_id is not None and options.conversation_id != owner_task["conversationId"]:
                raise ValueError(f"A child task lives in its owner's conversation {owner_task['conversationId']}")
        conversation_id = (
            owner_task["conversationId"] if owner_task is not None else options.conversation_id
        )
        if conversation_id is None:
            conversation_id = self._scope.conversation_id
        if conversation_id is None:
            raise TypeError("Tx.create_task() requires options.conversation_id")
        await self._require_conversation(conversation_id)
        self._assert_open()
        definition = task.definition
        checkpoint = definition.initial(copy_json(input))
        task_id = await self._host.storage.mint_id()
        self._assert_open()
        record = omit_none(
            {
                "id": task_id,
                "conversationId": conversation_id,
                "kind": definition.name,
                "version": definition.version,
                "input": copy_json(input),
                "owner": owner_task["id"] if owner_task is not None else None,
                "background": bool(options.background),
                "abortRequested": False,
                "abandonOnRestart": True if options.abandon_on_restart else None,
                "state": {"status": "pending", "checkpoint": copy_json(checkpoint)},
            }
        )
        self._tasks[task_id] = record
        self._created_task_ids.add(task_id)
        return task_id

    async def create_submission(self, create: Mapping[str, Any]) -> dict[str, Any]:
        self._has_table_write = True
        self._assert_open()
        await self._require_conversation(create["conversationId"])
        self._assert_open()
        submission_id = await self._host.storage.mint_id()
        self._assert_open()
        record = copy_json({**create, "id": submission_id})
        self._submissions[submission_id] = record
        return copy_json(record)

    def settle_submission(self, submission_id: int, settlement: Mapping[str, Any]) -> None:
        self._assert_open()
        self._has_table_write = True
        self._submission_changes.append((submission_id, copy_json(settlement)))

    def place_submission(self, submission_id: int, entry_id: int) -> None:
        self._assert_open()
        self._has_table_write = True
        self._submission_changes.append((submission_id, {"status": "placed", "entry": entry_id}))

    # ── Tasks (internal candidate writes) ──────────────────────────────────

    async def set_task(self, value: Mapping[str, Any]) -> None:
        self._assert_open()
        self._has_table_write = True
        task_id = value["id"]
        candidate = self._tasks.get(task_id)
        if candidate is not None and candidate["state"]["status"] == "terminal":
            raise ValueError(f"Task {task_id} already has a terminal candidate")
        if candidate is not None and candidate["conversationId"] != value["conversationId"]:
            raise ValueError(f"Task {task_id} cannot change conversations")
        record = copy_json(value)
        state = record["state"]
        if state["status"] == "running" and record.get("startedAt") is None:
            record["startedAt"] = self._host.now()
        if state["status"] == "terminal":
            record["endedAt"] = self._host.now()
            record.pop("memos", None)
        if state["status"] == "completing":
            record.pop("memos", None)
        self._tasks[task_id] = record

    def staged_tasks(self) -> list[dict[str, Any]]:
        return [copy_json(record) for record in self._tasks.values()]

    def staged_conversations(self) -> list[dict[str, Any]]:
        return [copy_json(write["value"]) for write in self._writes if write["type"] == "conversation"]

    # ── Documents ──────────────────────────────────────────────────────────

    async def doc(self, token: DocToken, *args: Any) -> DocumentDraft:
        """Get or create a document draft. Arguments: owner ID, family key, family seed."""
        self._assert_open()
        definition = token.definition
        resolved = resolve_address(definition, args)
        latest = self._latest_document_by_address.get(resolved.id)
        if latest is not None and not latest.retire_on_commit:
            if latest.draft_future is not None:
                return await latest.draft_future
            if latest.target is not None and latest.target["kind"] == "fork-copy":
                latest.draft_future = asyncio.ensure_future(
                    self._acquire_fork_copy(latest, definition, latest.target)
                )
                return await latest.draft_future
        seed = None
        if definition.family:
            if resolved.next_argument >= len(args):
                raise TypeError(f"Document family {definition.kind} requires a seed")
            seed = copy_json(args[resolved.next_argument])
        replaced = latest is not None and latest.retire_on_commit
        entry = _DocumentEntry(
            address_id=resolved.id,
            address=resolved.address,
            definition=definition,
        )
        self._documents.append(entry)
        self._latest_document_by_address[entry.address_id] = entry
        entry.draft_future = asyncio.ensure_future(self._acquire(entry, seed, skip_load=replaced))
        return await entry.draft_future

    async def retire_doc(self, token: DocToken, *args: Any) -> None:
        self._assert_open()
        definition = token.definition
        resolved = resolve_address(definition, args)
        latest = self._latest_document_by_address.get(resolved.id)
        if latest is not None and latest.retire_on_commit:
            return
        if latest is not None and latest.target is not None and latest.target["kind"] == "fork-copy":
            check_record_scope(definition, latest.target["record"])
            latest.retire_on_commit = True
            return
        if latest is not None and latest.draft_future is not None:
            latest.retire_on_commit = True
            await latest.draft_future
            return
        entry = _DocumentEntry(
            address_id=resolved.id,
            address=resolved.address,
            definition=definition,
            retire_on_commit=True,
        )
        self._documents.append(entry)
        self._latest_document_by_address[entry.address_id] = entry
        cached = self._host.cached_document(entry.address_id)
        record = cached.record if cached is not None else await self._host.storage.find_document(
            entry.address, "current"
        )
        self._assert_open()
        if record is None:
            return
        check_record_scope(definition, record)
        entry.target = {"kind": "retire-only", "record": copy_json(record)}

    async def _acquire(
        self, entry: _DocumentEntry, seed: Any, *, skip_load: bool
    ) -> DocumentDraft:
        definition = entry.definition
        assert definition is not None
        loaded = None if skip_load else await self._host.load_document(
            definition, entry.address_id, entry.address
        )
        self._assert_open()
        if loaded is not None:
            check_record_scope(definition, loaded.record)
            check_record_version(definition, loaded.record, loaded.stored_version)
            entry.target = {"kind": "loaded", "document": loaded}
            entry.change = loaded.tracker.begin_change()
            return begin_draft(entry.change)
        scope = entry.address["scope"]
        if scope["kind"] == "conversation":
            await self._require_conversation(scope["conversationId"])
        if scope["kind"] == "task":
            task = await self._current_task(scope["taskId"])
            if task is None:
                raise ValueError(f"Task {scope['taskId']} does not exist")
            if task["state"]["status"] == "terminal":
                raise ValueError(f"Task {scope['taskId']} is terminal")
        self._assert_open()
        value = definition.initial(seed) if definition.family else definition.initial()
        if not isinstance(value, dict):
            raise TypeError(f"Document {definition.kind} initial() must return a JSON object")
        value = copy_json(value)
        document_id = await self._host.storage.mint_id()
        self._assert_open()
        record = document_create(definition, entry.address, document_id)
        tracker = Tracker(value)
        entry.target = {"kind": "created", "record": record, "version": definition.version, "tracker": tracker}
        entry.change = tracker.begin_change()
        return begin_draft(entry.change)

    async def _acquire_fork_copy(
        self, entry: _DocumentEntry, definition: DocDefinition, target: Mapping[str, Any]
    ) -> DocumentDraft:
        stored = await self._host.storage.document(target["source"]["id"], target["source"]["at"])
        self._assert_open()
        if stored is None:
            raise ValueError(f"Fork source document {target['source']['id']} cannot be read")
        source = stored["record"]
        record = target["record"]
        if (
            source["scope"]["kind"] != "conversation"
            or source["kind"] != record["kind"]
            or source.get("key") != record.get("key")
            or source.get("history") != record.get("history")
            or source.get("fork") != record.get("fork")
        ):
            raise ValueError(f"Fork source document {target['source']['id']} does not match the copied record")
        value = materialize_document_value(definition, record, stored["version"], stored["value"])
        tracker = Tracker(value)
        entry.definition = definition
        entry.target = {
            "kind": "created",
            "record": record,
            "version": definition.version,
            "tracker": tracker,
        }
        entry.change = tracker.begin_change()
        return begin_draft(entry.change)

    # ── Settlement ─────────────────────────────────────────────────────────

    def _abort_changes(self) -> None:
        for entry in self._documents:
            if entry.change is not None and not entry.change.revoked:
                entry.change.abort()

    async def settle_failure(self) -> None:
        self._sealed = True
        pending = [entry.draft_future for entry in self._documents if entry.draft_future is not None]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._abort_changes()

    async def settle_success(self) -> list[StorageWrite]:
        self._sealed = True
        try:
            pending = [entry.draft_future for entry in self._documents if entry.draft_future is not None]
            if pending:
                results = await asyncio.gather(*pending, return_exceptions=True)
                for result in results:
                    if isinstance(result, BaseException):
                        raise result
            # Synchronously prepare every open change; this revokes every draft.
            for entry in self._documents:
                if entry.change is not None and not entry.change.revoked:
                    entry.prepared = entry.change.prepare()
            return await self._assemble()
        except BaseException:
            self._abort_changes()
            raise

    def discard(self) -> None:
        self._abort_changes()

    async def _assemble(self) -> list[StorageWrite]:
        storage = self._host.storage
        plans: list[_DocPlan] = []
        for entry in self._documents:
            plan = self._plan_document(entry)
            if plan is not None:
                plans.append(plan)
        self._reject_fork_source_writes(plans)
        await self._validate_owners()
        for task_id, candidate in self._tasks.items():
            committed = await self._committed_task(task_id)
            if committed is not None:
                if committed["state"]["status"] == "terminal":
                    raise ValueError(f"Task {task_id} is already terminal")
                if committed["conversationId"] != candidate["conversationId"]:
                    raise ValueError(f"Task {task_id} cannot change conversations")

        terminal_task_ids = {
            task_id for task_id, task in self._tasks.items() if task["state"]["status"] == "terminal"
        }
        if terminal_task_ids:
            retiring: set[int] = set()
            for plan in plans:
                scope = plan.record["scope"]
                if scope["kind"] == "task" and scope["taskId"] in terminal_task_ids:
                    plan.retire = True
                    retiring.add(plan.record["id"])
            for task_id in terminal_task_ids:
                candidate_task = self._tasks.get(task_id)
                if candidate_task is not None and candidate_task["state"]["status"] == "terminal":
                    # A task created in this transaction has no earlier documents.
                    cursor = None
                    while True:
                        page = await storage.scan_documents(
                            {"scope": {"kind": "task", "taskId": task_id}, "at": "current"}, 256, cursor
                        )
                        for record in page["items"]:
                            if record["id"] in retiring:
                                continue
                            plans.append(
                                _DocPlan(address_id=address_id(record), record=record, retire=True)
                            )
                            retiring.add(record["id"])
                        cursor = page.get("next")
                        if cursor is None:
                            break

        for plan in plans:
            scope = plan.record["scope"]
            if scope["kind"] == "conversation":
                plan.conversation_id = scope["conversationId"]
            elif scope["kind"] == "task":
                task = self._tasks.get(scope["taskId"])
                if task is None:
                    task = await self._current_task(scope["taskId"])
                if task is not None:
                    plan.conversation_id = task["conversationId"]

        for submission_id, change in self._submission_changes:
            current = self._submissions.get(submission_id)
            if current is None:
                current = await storage.submission(submission_id)
            if current is None:
                raise ValueError(f"Submission {submission_id} does not exist")
            updated = _apply_submission_change(current, change)
            if updated is not current:
                self._submissions[submission_id] = updated

        writes: list[StorageWrite] = list(self._writes)
        for submission in self._submissions.values():
            writes.append({"type": "submission", "value": submission})
        for task in self._tasks.values():
            writes.append({"type": "task", "value": task})
        for plan in plans:
            content = plan.content
            if (
                content is not None
                and content["type"] == "document.change"
                and content["content"]["kind"] == "delta"
                and plan.loaded is not None
                and plan.definition is not None
                and plan.prepared is not None
            ):
                info = CheckpointInfo(plan.loaded.deltas_since_base)
                if plan.definition.checkpoint_when(plan.prepared.value, plan.prepared.ops, info):
                    plan.content = {
                        **content,
                        "content": {
                            "version": plan.definition.version,
                            "kind": "base",
                            "value": plan.prepared.value,
                        },
                    }
            if plan.content is not None:
                writes.append(plan.content)
            if plan.retire:
                writes.append({"type": "document.retire", "id": plan.record["id"]})
        self._plans = plans
        return writes

    def _plan_document(self, entry: _DocumentEntry) -> _DocPlan | None:
        definition = entry.definition
        target = entry.target
        if target is None:
            if definition is None:
                return None
            if not entry.retire_on_commit:
                return None
            return None
        kind = target["kind"]
        if kind == "fork-copy":
            if entry.retire_on_commit:
                return None
            record = target["record"]
            return _DocPlan(
                address_id=entry.address_id,
                record=record,
                content={"type": "document.copy", "record": record, "source": target["source"]},
                definition=definition,
            )
        if kind == "retire-only":
            return _DocPlan(
                address_id=entry.address_id,
                record=target["record"],
                retire=True,
                definition=definition,
            )
        prepared = entry.prepared
        if prepared is None:
            return None
        if kind == "created":
            assert definition is not None
            record = copy_json(target["record"])
            plan = _DocPlan(
                address_id=entry.address_id,
                record=record,
                created=True,
                retire=entry.retire_on_commit,
                version=definition.version,
                definition=definition,
                change=entry.change,
                prepared=prepared,
                tracker=target["tracker"],
                content={
                    "type": "document.create",
                    "record": record,
                    "content": {"kind": "base", "version": definition.version, "value": prepared.value},
                },
            )
            return plan
        loaded: LoadedDocument = target["document"]
        assert definition is not None
        if not prepared.ops and not entry.retire_on_commit:
            return None
        plan = _DocPlan(
            address_id=entry.address_id,
            record=copy_json(loaded.record),
            retire=entry.retire_on_commit,
            version=definition.version,
            definition=definition,
            loaded=loaded,
            change=entry.change,
            prepared=prepared,
        )
        if prepared.ops:
            if loaded.stored_version != definition.version:
                content = {
                    "version": definition.version,
                    "kind": "base",
                    "value": prepared.value,
                }
            else:
                content = {
                    "version": definition.version,
                    "kind": "delta",
                    "ops": prepared.ops,
                }
            plan.content = {
                "type": "document.change",
                "id": loaded.record["id"],
                "content": content,
            }
        return plan

    def _reject_fork_source_writes(self, plans: Sequence[_DocPlan]) -> None:
        for plan in plans:
            if plan.record.get("id") in self._fork_source_document_ids:
                raise ValueError(f"Fork source document {plan.record['id']} is changed in the creating commit")

    async def _validate_owners(self) -> None:
        owners: list[tuple[str, int]] = []
        for write in self._writes:
            if write["type"] == "conversation" and write["value"].get("owner") is not None:
                owners.append(("Conversation owner task", write["value"]["owner"]["taskId"]))
        for task_id in self._created_task_ids:
            task = self._tasks.get(task_id)
            if task is not None and task.get("owner") is not None:
                owners.append(("Task owner", task["owner"]))
        for what, owner_id in owners:
            owner = await self._current_task(owner_id)
            if owner is None:
                raise ValueError(f"{what} {owner_id} does not exist")
            status = owner["state"]["status"]
            if status in ("terminal", "completing"):
                raise ValueError(f"{what} {owner_id} is {status}")
            if owner["abortRequested"]:
                raise ValueError(f"{what} {owner_id} is abort-marked")

    def adopt(self, seq: int) -> list[dict[str, Any]]:
        """Adopt prepared changes by pointer swap and describe document publications."""
        publications: list[dict[str, Any]] = []
        for plan in self._plans:
            record = copy_json(plan.record)
            if plan.created:
                record["createdAt"] = seq
            if plan.retire:
                record["retiredAt"] = seq
            prepared = plan.prepared
            if plan.loaded is not None:
                if prepared is not None and prepared.ops:
                    plan.loaded.tracker.adopt(prepared)
                    plan.loaded.stored_version = plan.version or plan.loaded.stored_version
                    if plan.content is not None and plan.content["type"] == "document.change":
                        if plan.content["content"]["kind"] == "base":
                            plan.loaded.deltas_since_base = 0
                        else:
                            plan.loaded.deltas_since_base += 1
                elif prepared is not None:
                    prepared.abort()
            elif plan.created and prepared is not None:
                if plan.retire:
                    prepared.abort()
                else:
                    tracker = plan.tracker if plan.tracker is not None else Tracker(prepared.value)
                    tracker.adopt(prepared)
                    self._host.install_document(
                        LoadedDocument(
                            address_id=plan.address_id,
                            record=record,
                            stored_version=plan.version or 1,
                            value_version=plan.version or 1,
                            deltas_since_base=0,
                            tracker=tracker,
                        )
                    )
            if plan.retire:
                self._host.evict_document(plan.address_id, record["id"])
            if plan.retire:
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
            elif plan.content is not None:
                publications.append(
                    {
                        "type": "document",
                        "record": record,
                        "conversationId": plan.conversation_id,
                        "version": plan.version,
                        "value": prepared.value if prepared is not None else {},
                        "ops": [] if plan.created or prepared is None else prepared.ops,
                    }
                )
        return publications

    # ── Helpers ────────────────────────────────────────────────────────────

    async def _require_conversation(self, conversation_id: int) -> None:
        if conversation_id in self._created_conversation_ids:
            return
        if await self._host.storage.conversation(conversation_id) is None:
            raise ValueError(f"Conversation {conversation_id} does not exist")

    async def _current_task(self, task_id: int) -> dict[str, Any] | None:
        candidate = self._tasks.get(task_id)
        if candidate is not None:
            return copy_json(candidate)
        return await self._committed_task(task_id)

    async def _committed_task(self, task_id: int) -> dict[str, Any] | None:
        if task_id in self._committed_tasks:
            cached = self._committed_tasks[task_id]
            return None if cached is None else copy_json(cached)
        task = await self._host.storage.task(task_id)
        self._committed_tasks[task_id] = task
        return None if task is None else copy_json(task)

    def _assert_open(self, *, allow_sealed: bool = False) -> None:
        if self._sealed and not allow_sealed:
            raise RuntimeError("Transaction is sealed")

    @property
    def has_table_write(self) -> bool:
        return self._has_table_write


def _apply_submission_change(current: Mapping[str, Any], change: Mapping[str, Any]) -> dict[str, Any]:
    """Complete record after applying one placement or settlement."""
    if current["status"] in ("done", "unanswered"):
        return dict(current)
    if change["status"] == "placed":
        if current["status"] != "queued":
            raise ValueError(f"Submission {current['id']} is not queued")
        status = "placed" if current["type"] == "input" else "done"
        return {**current, "status": status, "entry": change["entry"]}
    if change["status"] == "done":
        if current["status"] != "placed":
            raise ValueError(f"Submission {current['id']} is not a placed input")
        return {**current, "status": "done", "answer": change["answer"]}
    updated = dict(current)
    updated["status"] = "unanswered"
    updated["reason"] = change["reason"]
    if change.get("detail") is not None:
        updated["detail"] = copy_json(change["detail"])
    updated.pop("answer", None)
    if updated.get("entry") is None:
        updated.pop("entry", None)
    return updated


__all__ = ["Transaction", "TransactionHost"]
