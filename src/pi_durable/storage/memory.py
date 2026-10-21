"""In-memory reference Storage.

Reads and retained writes are cloned intentionally to match the ownership boundary of
serialization-backed stores. This is backend conformance, not validation.
"""

from __future__ import annotations

import bisect
from collections.abc import Mapping, Sequence
from typing import Any

from ..documents import address_id, is_alive_at
from ..errors import StorageRequestError
from ..json import copy_json
from ..types import Cursor, Page
from .base import (
    check_limit,
    clone,
    materialize_revisions,
    page,
    record_address_key,
    scan_indexes,
    scan_start,
    scope_key,
)


class MemoryStorage:
    """Detached, in-memory implementation of the Storage contract."""

    def __init__(self) -> None:
        self._record_types: dict[int, str] = {}
        self._conversations: dict[int, dict[str, Any]] = {}
        self._conversation_ids: list[int] = []
        self._entries: dict[int, dict[str, Any]] = {}
        self._entry_ids: dict[int, list[int]] = {}
        self._head_entry_ids: dict[int, list[int]] = {}
        self._entry_commit_seqs: dict[int, int] = {}
        self._tasks: dict[int, dict[str, Any]] = {}
        self._task_ids: list[int] = []
        self._submissions: dict[int, dict[str, Any]] = {}
        self._submission_ids: list[int] = []
        self._submission_ids_by_request: dict[tuple[int, str], int] = {}
        self._documents: dict[int, dict[str, Any]] = {}
        self._document_addresses: dict[str, dict[str, Any]] = {}
        self._document_ids_by_scope: dict[str, list[int]] = {}
        self._next_id = 2
        self._next_seq = 1
        self._closed = False

    # ── Commit ─────────────────────────────────────────────────────────────

    async def commit(self, writes: Sequence[Mapping[str, Any]]) -> int:
        self._assert_open()
        seq = self._next_seq
        detached = self._resolve_document_copies([copy_json(write) for write in writes])
        self._check_global_ids(detached)
        actions = self._prepare_document_actions(detached)
        self._check_document_actions(actions)
        self._apply(detached, actions, seq)
        self._next_seq = seq + 1
        return seq

    def _resolve_document_copies(self, writes: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not any(write["type"] == "document.copy" for write in writes):
            return writes
        changed: set[int] = set()
        for write in writes:
            if write["type"] in ("document.create", "document.copy"):
                changed.add(write["record"]["id"])
            elif write["type"] in ("document.change", "document.retire"):
                changed.add(write["id"])
        resolved: list[dict[str, Any]] = []
        for write in writes:
            if write["type"] != "document.copy":
                resolved.append(write)
                continue
            source = write["source"]
            if source["id"] in changed:
                raise ValueError(f"Fork source document {source['id']} is changed in the copy batch")
            stored = self._materialize_document(source["id"], source["at"])
            if stored is None:
                raise ValueError(f"Fork source document {source['id']} cannot be read")
            record = write["record"]
            source_record = stored["record"]
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
                {
                    "type": "document.create",
                    "record": record,
                    "content": {"kind": "base", "version": stored["version"], "value": stored["value"]},
                }
            )
        return resolved

    def _check_global_ids(self, writes: Sequence[Mapping[str, Any]]) -> None:
        claimed: dict[int, str] = {}
        for write in writes:
            kind = write["type"]
            if kind in ("document.change", "document.retire"):
                continue
            is_document = kind in ("document.create", "document.copy")
            table = "document" if is_document else kind
            record_id = write["record"]["id"] if is_document else write["value"]["id"]
            existing = self._record_types.get(record_id)
            earlier = claimed.get(record_id)
            if table in ("conversation", "entry", "document"):
                if existing is not None:
                    raise ValueError(f"ID {record_id} already belongs to {existing}")
                if earlier is not None:
                    raise ValueError(f"ID {record_id} is written more than once")
            else:
                if existing is not None and existing != table:
                    raise ValueError(f"ID {record_id} already belongs to {existing}")
                if earlier is not None and earlier != table:
                    raise ValueError(f"ID {record_id} is written as two record types")
            claimed[record_id] = table

    def _prepare_document_actions(
        self, writes: Sequence[Mapping[str, Any]]
    ) -> dict[int, dict[str, Any]]:
        actions: dict[int, dict[str, Any]] = {}
        for write in writes:
            kind = write["type"]
            if kind not in ("document.create", "document.change", "document.retire"):
                continue
            document_id = write["record"]["id"] if kind == "document.create" else write["id"]
            action = actions.setdefault(document_id, {"retire": False})
            if kind == "document.create":
                if "create" in action or "content" in action:
                    raise ValueError(f"Document {document_id} has more than one content command")
                action["create"] = write["record"]
                action["content"] = write["content"]
            elif kind == "document.change":
                if "content" in action:
                    raise ValueError(f"Document {document_id} has more than one content command")
                action["content"] = write["content"]
            else:
                if action["retire"]:
                    raise ValueError(f"Document {document_id} is retired more than once")
                action["retire"] = True
        return actions

    def _check_document_actions(self, actions: Mapping[int, Mapping[str, Any]]) -> None:
        live_counts: dict[str, int] = {}
        for document_id, action in actions.items():
            existing = self._documents.get(document_id)
            if "create" not in action and existing is None:
                raise ValueError(f"Unknown document: {document_id}")
            if "create" in action and existing is not None:
                raise ValueError(f"Document {document_id} already exists")
            if existing is not None and existing["record"].get("retiredAt") is not None:
                raise ValueError(f"Document {document_id} is retired")
            content = action.get("content")
            if content is not None and content["kind"] == "delta":
                revisions = existing["revisions"] if existing is not None else []
                if not revisions:
                    raise ValueError(f"Document {document_id} delta has no base")
                if revisions[-1]["version"] != content["version"]:
                    raise ValueError(f"Document {document_id} version transition requires a base")
            if "create" in action:
                key = record_address_key(action["create"])
            else:
                assert existing is not None
                key = record_address_key(existing["record"])
            current_id = self._document_addresses.get(key, {}).get("currentId")
            live = live_counts.get(key, 0 if current_id is None else 1)
            if action["retire"] and current_id == document_id:
                live -= 1
            if "create" in action and not action["retire"]:
                live += 1
            live_counts[key] = live
        for live in live_counts.values():
            if live > 1:
                raise ValueError("Document address already has a current incarnation")

    def _apply(
        self,
        writes: Sequence[Mapping[str, Any]],
        actions: Mapping[int, Mapping[str, Any]],
        seq: int,
    ) -> None:
        for write in writes:
            kind = write["type"]
            if kind == "conversation":
                value = write["value"]
                self._record_types[value["id"]] = "conversation"
                self._conversations[value["id"]] = value
                bisect.insort(self._conversation_ids, value["id"])
                self._next_id = max(self._next_id, value["id"] + 1)
            elif kind == "entry":
                value = write["value"]
                self._record_types[value["id"]] = "entry"
                self._entries[value["id"]] = value
                self._entry_commit_seqs[value["id"]] = seq
                bisect.insort(self._entry_ids.setdefault(value["conversationId"], []), value["id"])
                if value.get("head") is not None:
                    bisect.insort(self._head_entry_ids.setdefault(value["conversationId"], []), value["id"])
                self._next_id = max(self._next_id, value["id"] + 1)
            elif kind == "task":
                value = write["value"]
                self._record_types[value["id"]] = "task"
                if value["id"] not in self._tasks:
                    bisect.insort(self._task_ids, value["id"])
                self._tasks[value["id"]] = value
                self._next_id = max(self._next_id, value["id"] + 1)
            elif kind == "submission":
                value = write["value"]
                self._record_types[value["id"]] = "submission"
                previous = self._submissions.get(value["id"])
                if previous is None:
                    bisect.insort(self._submission_ids, value["id"])
                if previous is not None and previous.get("requestId") is not None:
                    key = (previous["conversationId"], previous["requestId"])
                    if self._submission_ids_by_request.get(key) == value["id"]:
                        del self._submission_ids_by_request[key]
                self._submissions[value["id"]] = value
                if value.get("requestId") is not None:
                    self._submission_ids_by_request[(value["conversationId"], value["requestId"])] = value["id"]
                self._next_id = max(self._next_id, value["id"] + 1)
        self._apply_document_actions(actions, seq)

    def _apply_document_actions(self, actions: Mapping[int, Mapping[str, Any]], seq: int) -> None:
        for document_id, action in actions.items():
            stored = self._documents.get(document_id)
            if "create" in action:
                record = dict(action["create"])
                record["createdAt"] = seq
                if action["retire"]:
                    record["retiredAt"] = seq
                stored = {"record": record, "revisions": [{**action["content"], "seq": seq}]}
                self._record_types[document_id] = "document"
                self._documents[document_id] = stored
                key = record_address_key(record)
                address = self._document_addresses.setdefault(key, {"ids": [], "currentId": None})
                bisect.insort(address["ids"], document_id)
                bisect.insort(self._document_ids_by_scope.setdefault(scope_key(record["scope"]), []), document_id)
                self._next_id = max(self._next_id, document_id + 1)
            elif "content" in action:
                revision = {**action["content"], "seq": seq}
                assert stored is not None
                from ..documents import is_current_only

                if revision["kind"] == "base" and is_current_only(stored["record"]):
                    stored["revisions"] = [revision]
                else:
                    stored["revisions"].append(revision)
            if action["retire"] and "create" not in action:
                assert stored is not None
                stored["record"] = {**stored["record"], "retiredAt": seq}
                from ..documents import is_current_only

                if is_current_only(stored["record"]):
                    stored["revisions"] = []
            if "create" in action or action["retire"]:
                assert stored is not None
                address = self._document_addresses[record_address_key(stored["record"])]
                if action["retire"] and address.get("currentId") == document_id:
                    address["currentId"] = None
                if "create" in action and not action["retire"]:
                    address["currentId"] = document_id

    async def mint_id(self) -> int:
        self._assert_open()
        value = self._next_id
        self._next_id += 1
        return value

    # ── Reads ──────────────────────────────────────────────────────────────

    async def conversation(self, conversation_id: int) -> dict[str, Any] | None:
        self._assert_open()
        value = self._conversations.get(conversation_id)
        return None if value is None else clone(value)

    async def scan_conversations(
        self, query: Mapping[str, Any], limit: int, cursor: Cursor | None = None
    ) -> Page:
        self._assert_open()
        check_limit(limit)
        order, after = scan_start(query.get("order"), cursor, "ascending")
        values: list[dict[str, Any]] = []
        for index in scan_indexes(self._conversation_ids, order, after):
            value = self._conversations[self._conversation_ids[index]]
            owner = value.get("owner")
            if query.get("ownerConversationId") is not None and (
                owner is None or owner.get("conversationId") != query["ownerConversationId"]
            ):
                continue
            if query.get("ownerTaskId") is not None and (
                owner is None or owner.get("taskId") != query["ownerTaskId"]
            ):
                continue
            values.append(value)
        return page(values, limit, order)

    async def entry(self, conversation_id: int, entry_id: int) -> Mapping[str, Any] | None:
        self._assert_open()
        entry = next(self._visible_entries(conversation_id, entry_id, entry_id), None)
        if entry is None:
            return None
        return {"entry": clone(entry), "commitSeq": self._entry_commit_seqs[entry_id]}

    async def entry_by_id(self, entry_id: int) -> dict[str, Any] | None:
        """Global entry lookup, for ``Tx.entry(id)``."""
        self._assert_open()
        entry = self._entries.get(entry_id)
        return None if entry is None else clone(entry)

    async def find_latest_head_marker(
        self, conversation_id: int, at_or_before: int | None = None
    ) -> Mapping[str, Any] | None:
        self._assert_open()
        if conversation_id not in self._conversations:
            raise StorageRequestError(f"Unknown conversation: {conversation_id}")
        current_id = conversation_id
        upper = at_or_before if at_or_before is not None else float("inf")
        while True:
            ids = self._head_entry_ids.get(current_id, [])
            index = bisect.bisect_right(ids, upper) - 1
            if index >= 0:
                entry = self._entries[ids[index]]
                return clone(entry)
            conversation = self._conversations[current_id]
            parent = conversation.get("parent")
            if parent is None:
                return None
            upper = min(upper, parent["at"])
            current_id = parent["conversationId"]

    async def scan_entries(
        self, query: Mapping[str, Any], limit: int, cursor: Cursor | None = None
    ) -> Page:
        self._assert_open()
        check_limit(limit)
        conversation_id = query["conversationId"]
        order, after = scan_start(query.get("order"), cursor, "descending")
        minimum = query.get("minEntryId")
        maximum = query.get("maxEntryId")
        if after is not None:
            if order == "descending":
                maximum = min(maximum if maximum is not None else after, after - 1)
            else:
                minimum = max(minimum if minimum is not None else after, after + 1)
        generator = (
            self._visible_entries(conversation_id, minimum, maximum)
            if order == "descending"
            else self._visible_entries_ascending(conversation_id, minimum, maximum)
        )
        values = [entry for entry in generator]
        return page(values, limit, order)

    async def task(self, task_id: int) -> dict[str, Any] | None:
        self._assert_open()
        value = self._tasks.get(task_id)
        return None if value is None else clone(value)

    async def scan_tasks(
        self, query: Mapping[str, Any], limit: int, cursor: Cursor | None = None
    ) -> Page:
        self._assert_open()
        check_limit(limit)
        order, after = scan_start(query.get("order"), cursor, "ascending")
        values: list[dict[str, Any]] = []
        for index in scan_indexes(self._task_ids, order, after):
            value = self._tasks[self._task_ids[index]]
            if query.get("conversationId") is not None and value["conversationId"] != query["conversationId"]:
                continue
            if query.get("kind") is not None and value["kind"] != query["kind"]:
                continue
            if query.get("status") is not None and value["state"]["status"] != query["status"]:
                continue
            if query.get("abortRequested") is not None and value["abortRequested"] != query["abortRequested"]:
                continue
            if query.get("background") is not None and value["background"] != query["background"]:
                continue
            values.append(value)
        return page(values, limit, order)

    async def submission(self, submission_id: int) -> dict[str, Any] | None:
        self._assert_open()
        value = self._submissions.get(submission_id)
        return None if value is None else clone(value)

    async def scan_submissions(
        self, query: Mapping[str, Any], limit: int, cursor: Cursor | None = None
    ) -> Page:
        self._assert_open()
        check_limit(limit)
        order, after = scan_start(query.get("order"), cursor, "ascending")
        values: list[dict[str, Any]] = []
        for index in scan_indexes(self._submission_ids, order, after):
            value = self._submissions[self._submission_ids[index]]
            if query.get("conversationId") is not None and value["conversationId"] != query["conversationId"]:
                continue
            if query.get("status") is not None and value["status"] != query["status"]:
                continue
            values.append(value)
        return page(values, limit, order)

    async def submission_by_request(self, conversation_id: int, request_id: str) -> dict[str, Any] | None:
        self._assert_open()
        submission_id = self._submission_ids_by_request.get((conversation_id, request_id))
        if submission_id is None:
            return None
        return clone(self._submissions[submission_id])

    async def find_document(self, address: Mapping[str, Any], at: int | str) -> dict[str, Any] | None:
        self._assert_open()
        index = self._document_addresses.get(address_id(address))
        if at == "current":
            current_id = None if index is None else index.get("currentId")
            return None if current_id is None else clone(self._documents[current_id]["record"])
        for document_id in [] if index is None else index["ids"]:
            record = self._documents[document_id]["record"]
            if is_alive_at(record, at):
                return clone(record)
        return None

    async def document(self, document_id: int, at: int | str) -> Mapping[str, Any] | None:
        self._assert_open()
        stored = self._materialize_document(document_id, at)
        return None if stored is None else clone(stored)

    async def scan_documents(
        self, query: Mapping[str, Any], limit: int, cursor: Cursor | None = None
    ) -> Page:
        self._assert_open()
        check_limit(limit)
        ids = self._document_ids_by_scope.get(scope_key(query["scope"]), [])
        after = cursor.get("after") if cursor is not None else None
        if after is not None and (not isinstance(after, int) or isinstance(after, bool)):
            raise StorageRequestError("Invalid storage cursor")
        start = 0 if after is None else bisect.bisect_right(ids, after)
        values: list[dict[str, Any]] = []
        for index in range(start, len(ids)):
            record = self._documents[ids[index]]["record"]
            if query.get("kind") is not None and record["kind"] != query["kind"]:
                continue
            if is_alive_at(record, query["at"]):
                values.append(record)
        return page(values, limit)

    async def close(self) -> None:
        self._closed = True

    # ── Helpers ────────────────────────────────────────────────────────────

    def _materialize_document(self, document_id: int, at: int | str) -> dict[str, Any] | None:
        stored = self._documents.get(document_id)
        if stored is None:
            return None
        result = materialize_revisions(document_id, stored["record"], stored["revisions"], at)
        if result is None:
            return None
        return result

    def _visible_entries(
        self,
        conversation_id: int,
        min_entry_id: int | None = None,
        max_entry_id: int | None = None,
    ):
        if conversation_id not in self._conversations:
            raise StorageRequestError(f"Unknown conversation: {conversation_id}")
        current_id = conversation_id
        upper = max_entry_id if max_entry_id is not None else float("inf")
        lower = min_entry_id if min_entry_id is not None else float("-inf")
        while True:
            ids = self._entry_ids.get(current_id, [])
            for index in range(bisect.bisect_right(ids, upper) - 1, -1, -1):
                entry_id = ids[index]
                if entry_id < lower:
                    break
                yield self._entries[entry_id]
            conversation = self._conversations[current_id]
            parent = conversation.get("parent")
            if parent is None:
                break
            upper = min(upper, parent["at"])
            if upper < lower:
                break
            current_id = parent["conversationId"]

    def _visible_entries_ascending(
        self,
        conversation_id: int,
        min_entry_id: int | None = None,
        max_entry_id: int | None = None,
    ):
        if conversation_id not in self._conversations:
            raise StorageRequestError(f"Unknown conversation: {conversation_id}")
        segments: list[tuple[int, float]] = []
        current_id = conversation_id
        upper = max_entry_id if max_entry_id is not None else float("inf")
        lower = min_entry_id if min_entry_id is not None else float("-inf")
        while True:
            segments.append((current_id, upper))
            conversation = self._conversations[current_id]
            parent = conversation.get("parent")
            if parent is None:
                break
            upper = min(upper, parent["at"])
            if upper < lower:
                break
            current_id = parent["conversationId"]
        for segment_id, segment_upper in reversed(segments):
            ids = self._entry_ids.get(segment_id, [])
            lower_index = bisect.bisect_left(ids, lower)
            for index in range(lower_index, len(ids)):
                entry_id = ids[index]
                if entry_id > segment_upper:
                    break
                yield self._entries[entry_id]

    def _assert_open(self) -> None:
        if self._closed:
            raise RuntimeError("MemoryStorage is closed")


__all__ = ["MemoryStorage"]
