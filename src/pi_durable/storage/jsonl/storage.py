"""Portable JSONL implementation of the storage contract, translated from v1.3.0.

Commits are append-only main markers plus document and live-task sidecars. In-memory state is
published only after the marker append succeeds; reopen replays confirmed records, truncates torn
or unconfirmed tails, and reclaims records that a later base or retirement made unnecessary.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, NotRequired, TypedDict, TypeGuard, cast, overload

from ...context import Context
from ...env import Err, FileError, FileSystem
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
from ...types import (
    ConversationQuery,
    ConversationRecord,
    Cursor,
    DocumentAddress,
    DocumentCreate,
    DocumentPoint,
    DocumentQuery,
    DocumentRecord,
    EntryLookup,
    EntryQuery,
    EntryRecord,
    EntryWithHead,
    Page,
    StorageWrite,
    StoredDocument,
    SubmissionQuery,
    SubmissionRecord,
    TaskQuery,
    TaskRecord,
)
from ..memory import MemoryStorage

FORMAT_VERSION = 1
MAIN_FILE = "main.jsonl"
RECLAIM_SUFFIX = ".reclaim"

_SIDECAR_NAME = re.compile(r"^(?:doc|task)-(?P<id>0|[1-9]\d*)\.jsonl$")
_RECLAIM_NAME = re.compile(r"^(?:doc|task)-(?:0|[1-9]\d*)\.jsonl\.reclaim$")
_MAX_SAFE_INTEGER = 9_007_199_254_740_991
_MAIN_ONLY_WRITES = frozenset({"conversation", "entry", "submission", "document.retire"})
_MAIN_AND_TASK_WRITES = _MAIN_ONLY_WRITES | {"task"}


class JsonlStorageOptions(TypedDict):
    """Options for opening a JSONL directory."""

    fsync: NotRequired[bool]


class JsonlCorruptionError(Exception):
    """Committed JSONL state is inconsistent; opening the backend fails."""

    def __init__(self, message: str, cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.name = "JsonlCorruptionError"
        if cause is not None:
            self.__cause__ = cause


class JsonlStoragePoisonedError(Exception):
    """An append outcome is uncertain, so the open backend must be reopened."""

    def __init__(self, cause: BaseException) -> None:
        super().__init__("JSONL storage is poisoned and must be reopened")
        self.name = "JsonlStoragePoisonedError"
        self.__cause__ = cause


@dataclass(frozen=True, slots=True)
class _ParsedLine:
    value: Any
    start: int


@dataclass(frozen=True, slots=True)
class _ParsedFile:
    path: str
    lines: list[_ParsedLine]


@dataclass(frozen=True, slots=True)
class _EncodedCommit:
    marker: str
    sidecars: dict[str, str]


def _is_object(value: object) -> TypeGuard[dict[str, Any]]:
    return isinstance(value, dict)


def _is_safe_int(value: object) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool) and abs(value) <= _MAX_SAFE_INTEGER


def _sidecar_file_name(kind: str, id: int) -> str:
    return f"{kind}-{id}.jsonl"


def _is_sidecar_file_name(name: str) -> bool:
    return _SIDECAR_NAME.match(name) is not None


def _sidecar_file_id(name: str) -> int:
    match = _SIDECAR_NAME.match(name)
    assert match is not None
    return int(match.group("id"))


def _is_reclaim_file_name(name: str) -> bool:
    return _RECLAIM_NAME.match(name) is not None


def _is_current_only(record: DocumentCreate | DocumentRecord) -> bool:
    return record["scope"]["kind"] != "conversation" or record.get("history") == "latest"


def _sidecar_key(file: str, seq: int, ordinal: int) -> str:
    return json.dumps([file, seq, ordinal], separators=(",", ":"))


def _json_line(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), allow_nan=False) + "\n"


def _error_from_file(action: str, error: FileError) -> RuntimeError:
    wrapped = RuntimeError(f"JSONL {action} failed: {error}")
    wrapped.__cause__ = error
    return wrapped


def _parse_json(text: str, description: str) -> object:
    try:
        return json.loads(text)
    except json.JSONDecodeError as error:
        raise JsonlCorruptionError(f"Malformed complete {description}", error) from error


def _validate_main_operation(value: object, description: str) -> dict[str, Any]:
    if not _is_object(value) or not isinstance(value.get("type"), str):
        raise JsonlCorruptionError(f"Invalid write in {description}")
    operation = value
    kind = operation["type"]
    if kind in ("conversation", "entry", "submission"):
        record = operation.get("value")
        if not _is_object(record) or not _is_safe_int(record.get("id")):
            raise JsonlCorruptionError(f"Invalid {kind} write in {description}")
        return operation
    if kind == "task":
        record = operation.get("value")
        if (
            not _is_object(record)
            or not _is_safe_int(record.get("id"))
            or not _is_object(record.get("state"))
            or record["state"].get("status") != "terminal"
        ):
            raise JsonlCorruptionError(f"Invalid terminal task write in {description}")
        return operation
    if kind == "document.retire":
        if not _is_safe_int(operation.get("id")):
            raise JsonlCorruptionError(f"Invalid document retirement in {description}")
        return operation
    if kind == "task.sidecar":
        if (
            not _is_safe_int(operation.get("id"))
            or not _is_safe_int(operation.get("ordinal"))
            or operation["ordinal"] < 0
        ):
            raise JsonlCorruptionError(f"Invalid task sidecar write in {description}")
        return operation
    if kind == "document.create":
        record = operation.get("record")
        if (
            not _is_object(record)
            or not _is_safe_int(record.get("id"))
            or not _is_safe_int(operation.get("ordinal"))
            or operation["ordinal"] < 0
        ):
            raise JsonlCorruptionError(f"Invalid document creation in {description}")
        return operation
    if kind == "document.change":
        if (
            not _is_safe_int(operation.get("id"))
            or not _is_safe_int(operation.get("ordinal"))
            or operation["ordinal"] < 0
        ):
            raise JsonlCorruptionError(f"Invalid document change in {description}")
        return operation
    raise JsonlCorruptionError(f"Unknown write type in {description}")


def _parse_main_marker(text: str, line: int) -> dict[str, Any]:
    description = f"{MAIN_FILE} line {line}"
    value = _parse_json(text, description)
    if (
        not _is_object(value)
        or not _is_safe_int(value.get("format"))
        or value["format"] != FORMAT_VERSION
        or value.get("type") != "commit"
        or not _is_safe_int(value.get("seq"))
        or value["seq"] < 1
        or not isinstance(value.get("writes"), list)
    ):
        raise JsonlCorruptionError(f"Invalid commit marker in {description}")
    return {
        "format": FORMAT_VERSION,
        "type": "commit",
        "seq": seq_from_number(value["seq"]),
        "writes": [_validate_main_operation(write, description) for write in value["writes"]],
    }


def _validate_document_content(value: object, description: str) -> dict[str, Any]:
    if not _is_object(value) or not _is_safe_int(value.get("version")) or value["version"] < 1:
        raise JsonlCorruptionError(f"Invalid document content in {description}")
    if value.get("kind") == "base" and _is_object(value.get("value")):
        return value
    if value.get("kind") == "delta" and isinstance(value.get("ops"), list):
        return value
    raise JsonlCorruptionError(f"Invalid document content in {description}")


def _parse_sidecar_record(text: str, file: str, line: int) -> dict[str, Any]:
    description = f"{file} line {line}"
    value = _parse_json(text, description)
    if (
        not _is_object(value)
        or not _is_safe_int(value.get("format"))
        or value["format"] != FORMAT_VERSION
        or value.get("type") != "record"
        or not _is_safe_int(value.get("seq"))
        or value["seq"] < 1
        or not _is_safe_int(value.get("ordinal"))
        or value["ordinal"] < 0
        or not _is_object(value.get("payload"))
        or not isinstance(value["payload"].get("type"), str)
    ):
        raise JsonlCorruptionError(f"Invalid sidecar record in {description}")
    payload = value["payload"]
    if payload["type"] == "task":
        record = payload.get("value")
        if (
            not _is_object(record)
            or not _is_safe_int(record.get("id"))
            or not _is_object(record.get("state"))
            or record["state"].get("status") == "terminal"
        ):
            raise JsonlCorruptionError(f"Invalid live task record in {description}")
    elif payload["type"] == "document":
        if not _is_safe_int(payload.get("id")):
            raise JsonlCorruptionError(f"Invalid document record in {description}")
        _validate_document_content(payload.get("content"), description)
    else:
        raise JsonlCorruptionError(f"Unknown sidecar record type in {description}")
    return value


class JsonlStorage:
    """Portable JSONL implementation of the storage contract."""

    def __init__(
        self,
        fs: FileSystem,
        directory: str,
        main_path: str,
        options: JsonlStorageOptions | None,
    ) -> None:
        self._fs = fs
        self._directory = directory
        self._main_path = main_path
        self._fsync = bool(options and options.get("fsync", False))
        self._memory = MemoryStorage()
        self._current_only_documents: set[int] = set()
        self._live_task_sidecars: set[int] = set()
        self._closed = False
        self._poison_error: JsonlStoragePoisonedError | None = None

    @staticmethod
    async def open(
        directory: str,
        fs: FileSystem,
        context: Context,
        options: JsonlStorageOptions | None = None,
    ) -> JsonlStorage:
        """Open or create a JSONL storage directory using the supplied filesystem."""

        absolute = await fs.absolute_path(directory, context)
        if isinstance(absolute, Err):
            raise _error_from_file("path resolution", absolute.error)
        created = await fs.create_dir(absolute.value, {"recursive": True}, context)
        if isinstance(created, Err):
            raise _error_from_file("directory creation", created.error)
        main_path = await fs.join_path([absolute.value, MAIN_FILE], context)
        if isinstance(main_path, Err):
            raise _error_from_file("path join", main_path.error)
        storage = JsonlStorage(fs, absolute.value, main_path.value, options)
        await storage._recover(context)
        return storage

    async def commit(self, writes: Sequence[StorageWrite], context: Context) -> Seq:
        self._assert_usable()
        prepared = self._memory.prepare_commit(writes)
        encoded = self._encode_commit(prepared.seq, prepared.writes)
        reclamations = self._plan_reclamations(prepared.writes, encoded)
        sidecars: list[tuple[str, str, str]] = [
            (file, content, await self._resolve_file(file, context))
            for file, content in encoded.sidecars.items()
        ]

        for file, content, path in sidecars:
            result = await self._fs.append_file(path, content, context)
            if isinstance(result, Err):
                raise self._poison(_error_from_file(f"append to {file}", result.error))
        if self._fsync:
            for file, _content, path in sidecars:
                result = await self._fs.flush_file(path, context)
                if isinstance(result, Err):
                    raise self._poison(_error_from_file(f"flush of {file}", result.error))
        marker = await self._fs.append_file(self._main_path, encoded.marker, context)
        if isinstance(marker, Err):
            raise self._poison(_error_from_file(f"append to {MAIN_FILE}", marker.error))
        seq = prepared.apply()
        self._adopt_sidecar_state(prepared.writes)
        await self._reclaim_sidecars(reclamations, context)
        return seq

    async def mint_id(self) -> int:
        return await self._store.mint_id()

    async def conversation(self, id: ConversationId, context: Context) -> ConversationRecord | None:
        return await self._store.conversation(id, context)

    async def scan_conversations(
        self,
        query: ConversationQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[ConversationRecord, Cursor]:
        return await self._store.scan_conversations(query, limit, cursor, context)

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
        if context is None:
            if not isinstance(id_or_context, Context) and id_or_context is not None:
                raise StorageRequestError("Storage.entry() requires an entry ID")
            return await self._store.entry(id_from_number(id_or_conversation_id), id_or_context)
        if not _is_safe_int(id_or_context):
            raise StorageRequestError("Storage.entry() requires an entry ID")
        return await self._store.entry(
            id_from_number(id_or_conversation_id),
            id_from_number(id_or_context),
            context,
        )

    async def find_latest_head_marker(
        self,
        conversation_id: ConversationId,
        at_or_before_entry_id: EntryId | None,
        context: Context,
    ) -> EntryWithHead | None:
        return await self._store.find_latest_head_marker(
            conversation_id, at_or_before_entry_id, context
        )

    async def scan_entries(
        self,
        query: EntryQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[EntryRecord, Cursor]:
        return await self._store.scan_entries(query, limit, cursor, context)

    async def task(self, id: TaskId, context: Context) -> TaskRecord | None:
        return await self._store.task(id, context)

    async def scan_tasks(
        self,
        query: TaskQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[TaskRecord, Cursor]:
        return await self._store.scan_tasks(query, limit, cursor, context)

    async def submission(self, id: SubmissionId, context: Context) -> SubmissionRecord | None:
        return await self._store.submission(id, context)

    async def scan_submissions(
        self,
        query: SubmissionQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[SubmissionRecord, Cursor]:
        return await self._store.scan_submissions(query, limit, cursor, context)

    async def submission_by_request(
        self,
        conversation_id: ConversationId,
        request_id: str,
        context: Context,
    ) -> SubmissionRecord | None:
        return await self._store.submission_by_request(conversation_id, request_id, context)

    async def find_document(
        self,
        address: DocumentAddress,
        at: DocumentPoint,
        context: Context,
    ) -> DocumentRecord | None:
        return await self._store.find_document(address, at, context)

    async def document(
        self,
        id: DocumentId,
        at: DocumentPoint,
        context: Context,
    ) -> StoredDocument | None:
        return await self._store.document(id, at, context)

    async def scan_documents(
        self,
        query: DocumentQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[DocumentRecord, Cursor]:
        return await self._store.scan_documents(query, limit, cursor, context)

    async def close(self, context: Context) -> None:
        if self._closed:
            return
        self._closed = True
        await self._memory.close(context)

    # ── Encoding and publication ────────────────────────────────────────────

    def _encode_commit(self, seq: Seq, writes: Sequence[StorageWrite]) -> _EncodedCommit:
        main_writes: list[dict[str, Any]] = []
        records: dict[str, list[dict[str, Any]]] = {}
        next_ordinal = 0

        def add_sidecar(file: str, payload: dict[str, Any]) -> int:
            nonlocal next_ordinal
            ordinal = next_ordinal
            next_ordinal += 1
            records.setdefault(file, []).append(
                {
                    "format": FORMAT_VERSION,
                    "type": "record",
                    "seq": seq,
                    "ordinal": ordinal,
                    "payload": payload,
                }
            )
            return ordinal

        for write in writes:
            kind = write["type"]
            if kind in _MAIN_ONLY_WRITES:
                main_writes.append(cast(dict[str, Any], write))
            elif kind == "task":
                task = cast(dict[str, Any], write["value"])
                if task["state"]["status"] == "terminal":
                    main_writes.append(cast(dict[str, Any], write))
                else:
                    ordinal = add_sidecar(
                        _sidecar_file_name("task", task["id"]),
                        {"type": "task", "value": task},
                    )
                    main_writes.append({"type": "task.sidecar", "id": task["id"], "ordinal": ordinal})
            elif kind == "document.create":
                record = cast(dict[str, Any], write["record"])
                ordinal = add_sidecar(
                    _sidecar_file_name("doc", record["id"]),
                    {"type": "document", "id": record["id"], "content": write["content"]},
                )
                main_writes.append(
                    {"type": "document.create", "record": record, "ordinal": ordinal}
                )
            elif kind == "document.change":
                id = cast(int, write["id"])
                ordinal = add_sidecar(
                    _sidecar_file_name("doc", id),
                    {"type": "document", "id": id, "content": write["content"]},
                )
                main_writes.append({"type": "document.change", "id": id, "ordinal": ordinal})

        sidecars = {
            file: "".join(_json_line(record) for record in file_records)
            for file, file_records in records.items()
        }
        marker = _json_line(
            {"format": FORMAT_VERSION, "type": "commit", "seq": seq, "writes": main_writes}
        )
        return _EncodedCommit(marker=marker, sidecars=sidecars)

    def _plan_reclamations(
        self,
        writes: Sequence[StorageWrite],
        encoded: _EncodedCommit,
    ) -> dict[str, str]:
        created_current_only: dict[int, None] = {}
        retired_documents: dict[int, None] = {}
        base_documents: dict[int, None] = {}
        final_tasks: dict[int, dict[str, Any]] = {}
        for write in writes:
            kind = write["type"]
            if kind == "document.create":
                record = cast(dict[str, Any], write["record"])
                if _is_current_only(cast(DocumentCreate, record)):
                    created_current_only[record["id"]] = None
            elif kind == "document.change":
                if write["content"]["kind"] == "base":
                    base_documents[cast(int, write["id"])] = None
            elif kind == "document.retire":
                retired_documents[cast(int, write["id"])] = None
            elif kind == "task":
                task = cast(dict[str, Any], write["value"])
                final_tasks[task["id"]] = task

        replacements: dict[str, str] = {}

        def is_current_only_document(id: int) -> bool:
            return id in self._current_only_documents or id in created_current_only

        for id in retired_documents:
            if is_current_only_document(id):
                replacements[_sidecar_file_name("doc", id)] = ""
        for id in base_documents:
            if not is_current_only_document(id) or id in retired_documents:
                continue
            file = _sidecar_file_name("doc", id)
            content = encoded.sidecars.get(file)
            if content is not None:
                replacements[file] = content
        for id, task in final_tasks.items():
            if task["state"]["status"] == "terminal" and (
                id in self._live_task_sidecars
                or _sidecar_file_name("task", id) in encoded.sidecars
            ):
                replacements[_sidecar_file_name("task", id)] = ""
        return replacements

    def _adopt_sidecar_state(self, writes: Sequence[StorageWrite]) -> None:
        for write in writes:
            if write["type"] == "document.create":
                record = cast(dict[str, Any], write["record"])
                if _is_current_only(cast(DocumentCreate, record)):
                    self._current_only_documents.add(record["id"])
            elif write["type"] == "task":
                task = cast(dict[str, Any], write["value"])
                if task["state"]["status"] == "terminal":
                    self._live_task_sidecars.discard(task["id"])
                else:
                    self._live_task_sidecars.add(task["id"])

    async def _reclaim_sidecars(self, replacements: dict[str, str], context: Context) -> None:
        """The marker already published this state, so reclamation is retryable maintenance."""

        if not replacements:
            return
        if self._fsync:
            flushed = await self._fs.flush_file(self._main_path, context)
            if isinstance(flushed, Err):
                return
        for file, content in replacements.items():
            await self._replace_sidecar(file, content, context)

    async def _replace_sidecar(self, file: str, content: str, context: Context) -> None:
        path = await self._try_join([self._directory, file], context)
        if path is None:
            return
        if content == "":
            await self._fs.remove(path, {"force": True}, context)
            return
        temporary_path = await self._try_join([self._directory, f"{file}{RECLAIM_SUFFIX}"], context)
        if temporary_path is None:
            return
        written = await self._fs.write_file(temporary_path, content, context)
        if isinstance(written, Err):
            return
        if self._fsync:
            flushed = await self._fs.flush_file(temporary_path, context)
            if isinstance(flushed, Err):
                return
        await self._fs.rename_file(temporary_path, path, context)

    # ── Recovery ────────────────────────────────────────────────────────────

    async def _recover(self, context: Context) -> None:
        main = await self._read_lines(
            self._main_path,
            MAIN_FILE,
            context,
            lambda text, line: _parse_main_marker(text, line),
        )
        previous_seq = 0
        for marker in main.lines:
            if marker.value["seq"] <= previous_seq:
                raise JsonlCorruptionError(
                    f"Commit sequence does not strictly increase in {MAIN_FILE}"
                )
            previous_seq = marker.value["seq"]

        listed = await self._fs.list_dir(self._directory, context)
        if isinstance(listed, Err):
            raise _error_from_file("directory listing", listed.error)
        for info in listed.value:
            if info["kind"] == "file" and _is_reclaim_file_name(info["name"]):
                await self._fs.remove(info["path"], {"force": True}, context)
        sidecar_files = sorted(
            info["name"]
            for info in listed.value
            if info["kind"] == "file" and _is_sidecar_file_name(info["name"])
        )
        parsed_files: dict[str, _ParsedFile] = {}
        record_by_key: dict[str, _ParsedLine] = {}
        for file in sidecar_files:
            path = await self._try_join([self._directory, file], context)
            if path is None:
                raise _error_from_file("path join", FileError("invalid", "Invalid path", file))
            parsed = await self._read_lines(
                path,
                file,
                context,
                lambda text, line, file=file: _parse_sidecar_record(text, file, line),
            )
            parsed_files[file] = parsed
            previous: dict[str, Any] | None = None
            for line in parsed.lines:
                value = line.value
                if previous is not None and (
                    value["seq"] < previous["seq"]
                    or (value["seq"] == previous["seq"] and value["ordinal"] <= previous["ordinal"])
                ):
                    raise JsonlCorruptionError(f"Sidecar records are out of order in {file}")
                previous = value
                record_by_key[_sidecar_key(file, value["seq"], value["ordinal"])] = line

        current_only_documents: dict[int, None] = {}
        retired_documents: dict[int, None] = {}
        final_task_is_live: dict[int, bool] = {}
        for marker_line in main.lines:
            for operation in marker_line.value["writes"]:
                kind = operation["type"]
                if kind == "document.create":
                    if _is_current_only(operation["record"]):
                        current_only_documents[operation["record"]["id"]] = None
                elif kind == "document.retire":
                    retired_documents[operation["id"]] = None
                elif kind == "task":
                    final_task_is_live[operation["value"]["id"]] = False
                elif kind == "task.sidecar":
                    final_task_is_live[operation["id"]] = True
        retired_current_only = {
            id: None for id in retired_documents if id in current_only_documents
        }

        latest_bases: dict[int, dict[str, Any]] = {}
        for marker_line in main.lines:
            marker = marker_line.value
            for operation in marker["writes"]:
                kind = operation["type"]
                if kind not in ("document.create", "document.change"):
                    continue
                id = operation["record"]["id"] if kind == "document.create" else operation["id"]
                if id not in current_only_documents:
                    continue
                record_line = record_by_key.get(
                    _sidecar_key(_sidecar_file_name("doc", id), marker["seq"], operation["ordinal"])
                )
                if record_line is None:
                    continue
                record = record_line.value
                if (
                    record["payload"]["type"] != "document"
                    or record["payload"]["id"] != id
                    or record["payload"]["content"]["kind"] != "base"
                ):
                    continue
                previous = latest_bases.get(id)
                if (
                    previous is None
                    or record["seq"] > previous["seq"]
                    or (record["seq"] == previous["seq"] and record["ordinal"] > previous["ordinal"])
                ):
                    latest_bases[id] = record

        terminal_tasks = {id for id, live in final_task_is_live.items() if not live}
        confirmed: set[str] = set()
        for marker_line in main.lines:
            marker = marker_line.value
            writes: list[dict[str, Any]] = []
            for operation in marker["writes"]:
                kind = operation["type"]
                if kind in _MAIN_AND_TASK_WRITES:
                    writes.append(operation)
                elif kind == "task.sidecar":
                    optional = operation["id"] in terminal_tasks
                    record = self._confirm_record(
                        marker,
                        operation["ordinal"],
                        _sidecar_file_name("task", operation["id"]),
                        record_by_key,
                        confirmed,
                        optional,
                    )
                    if record is not None:
                        if (
                            record["payload"]["type"] != "task"
                            or record["payload"]["value"]["id"] != operation["id"]
                        ):
                            raise JsonlCorruptionError(
                                f"Confirmed task sidecar data does not match commit {marker['seq']}"
                            )
                        if not optional:
                            writes.append({"type": "task", "value": record["payload"]["value"]})
                else:
                    id = (
                        operation["record"]["id"]
                        if kind == "document.create"
                        else operation["id"]
                    )
                    reclaimed = id in retired_current_only or self._is_before_latest_base(
                        latest_bases, id, marker["seq"], operation["ordinal"]
                    )
                    record = self._confirm_record(
                        marker,
                        operation["ordinal"],
                        _sidecar_file_name("doc", id),
                        record_by_key,
                        confirmed,
                        reclaimed,
                    )
                    content: dict[str, Any] | None = None
                    if record is not None:
                        if (
                            record["payload"]["type"] != "document"
                            or record["payload"]["id"] != id
                        ):
                            raise JsonlCorruptionError(
                                f"Confirmed document sidecar data does not match commit {marker['seq']}"
                            )
                        content = record["payload"]["content"]
                    if kind == "document.create":
                        if content is not None and content["kind"] != "base":
                            raise JsonlCorruptionError(
                                f"Document creation lacks a confirmed base in commit {marker['seq']}"
                            )
                        writes.append(
                            {
                                "type": "document.create",
                                "record": operation["record"],
                                "content": (
                                    {"kind": "base", "version": 1, "value": {}}
                                    if reclaimed or content is None
                                    else content
                                ),
                            }
                        )
                    elif not reclaimed and content is not None:
                        writes.append(
                            {"type": "document.change", "id": id, "content": content}
                        )
            try:
                self._memory.prepare_commit(cast(list[StorageWrite], writes), marker["seq"]).apply()
            except Exception as error:
                raise JsonlCorruptionError(
                    f"Invalid committed state at sequence {marker['seq']}", error
                ) from error

        reclamations: dict[str, str] = {}
        for file, parsed in parsed_files.items():
            unconfirmed_at: int | None = None
            for line in parsed.lines:
                key = _sidecar_key(file, line.value["seq"], line.value["ordinal"])
                if key in confirmed:
                    if unconfirmed_at is not None:
                        raise JsonlCorruptionError(
                            f"Confirmed record follows an unconfirmed tail in {file}"
                        )
                elif unconfirmed_at is None:
                    unconfirmed_at = line.start
            if unconfirmed_at is not None:
                truncated = await self._fs.truncate_file(parsed.path, unconfirmed_at, context)
                if isinstance(truncated, Err):
                    raise _error_from_file(f"tail truncation of {file}", truncated.error)

            numeric_id = _sidecar_file_id(file)
            confirmed_lines = [
                line
                for line in parsed.lines
                if _sidecar_key(file, line.value["seq"], line.value["ordinal"]) in confirmed
            ]
            retained_lines: list[_ParsedLine] | None = None
            if file.startswith("task-") and numeric_id in terminal_tasks:
                retained_lines = []
            elif file.startswith("doc-"):
                document_id = numeric_id
                if document_id in retired_current_only:
                    retained_lines = []
                elif document_id in latest_bases:
                    retained_lines = [
                        line
                        for line in confirmed_lines
                        if not self._is_before_latest_base(
                            latest_bases, document_id, line.value["seq"], line.value["ordinal"]
                        )
                    ]
            if retained_lines is not None and (
                len(retained_lines) < len(confirmed_lines) or len(retained_lines) == 0
            ):
                reclamations[file] = "".join(_json_line(line.value) for line in retained_lines)
        await self._reclaim_sidecars(reclamations, context)

        self._current_only_documents.update(current_only_documents)
        for id, live in final_task_is_live.items():
            if live:
                self._live_task_sidecars.add(id)

    @staticmethod
    def _confirm_record(
        marker: dict[str, Any],
        ordinal: int,
        file: str,
        record_by_key: dict[str, _ParsedLine],
        confirmed: set[str],
        optional: bool,
    ) -> dict[str, Any] | None:
        key = _sidecar_key(file, marker["seq"], ordinal)
        if key in confirmed:
            raise JsonlCorruptionError("Sidecar record is confirmed more than once")
        line = record_by_key.get(key)
        if line is None:
            if optional:
                return None
            raise JsonlCorruptionError(
                f"Missing confirmed sidecar record {file} at sequence {marker['seq']}"
            )
        confirmed.add(key)
        return cast(dict[str, Any], line.value)

    @staticmethod
    def _is_before_latest_base(
        latest_bases: dict[int, dict[str, Any]],
        id: int,
        seq: int,
        ordinal: int,
    ) -> bool:
        base = latest_bases.get(id)
        return base is not None and (
            seq < base["seq"] or (seq == base["seq"] and ordinal < base["ordinal"])
        )

    async def _read_lines(
        self,
        path: str,
        name: str,
        context: Context,
        parse: Callable[[str, int], Any],
    ) -> _ParsedFile:
        read = await self._fs.read_binary_file(path, context)
        if isinstance(read, Err):
            if read.error.code == "not_found":
                return _ParsedFile(path, [])
            raise _error_from_file(f"read of {name}", read.error)
        data = read.value
        complete_size = len(data)
        if complete_size > 0 and data[complete_size - 1] != 0x0A:
            complete_size = data.rfind(b"\n") + 1
            truncated = await self._fs.truncate_file(path, complete_size, context)
            if isinstance(truncated, Err):
                raise _error_from_file(f"torn-line truncation of {name}", truncated.error)
        lines: list[_ParsedLine] = []
        start = 0
        line_number = 1
        for raw in data[:complete_size].split(b"\n")[:-1]:
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError as error:
                raise JsonlCorruptionError(
                    f"Invalid UTF-8 in complete {name} line {line_number}", error
                ) from error
            lines.append(_ParsedLine(parse(text, line_number), start))
            start += len(raw) + 1
            line_number += 1
        return _ParsedFile(path, lines)

    async def _resolve_file(self, file: str, context: Context) -> str:
        path = await self._fs.join_path([self._directory, file], context)
        if isinstance(path, Err):
            raise _error_from_file("path join", path.error)
        return path.value

    async def _try_join(self, parts: Sequence[str], context: Context) -> str | None:
        path = await self._fs.join_path(parts, context)
        return None if isinstance(path, Err) else path.value

    @property
    def _store(self) -> MemoryStorage:
        self._assert_usable()
        return self._memory

    def _poison(self, cause: Exception) -> JsonlStoragePoisonedError:
        if self._poison_error is None:
            self._poison_error = JsonlStoragePoisonedError(cause)
        return self._poison_error

    def _assert_usable(self) -> None:
        if self._closed:
            raise RuntimeError("JsonlStorage is closed")
        if self._poison_error is not None:
            raise self._poison_error


__all__ = [
    "JsonlCorruptionError",
    "JsonlStorage",
    "JsonlStorageOptions",
    "JsonlStoragePoisonedError",
]
