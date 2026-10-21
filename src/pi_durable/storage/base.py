"""Storage protocol and shared scan machinery."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Protocol, runtime_checkable

from ..documents import address_id, is_alive_at, is_current_only
from ..errors import StorageRequestError
from ..json import copy_json
from ..types import Cursor, Page, ScanOrder, make_page, next_cursor


@runtime_checkable
class Storage(Protocol):
    """Atomic persistence boundary for Session records.

    Storage trusts the owning Session to supply semantically valid records, references,
    ancestry, and transitions. Implementations enforce atomicity, global ID ownership,
    immutable conversation/entry creation, document consistency, and detached values.
    """

    async def commit(self, writes: Sequence[Mapping[str, Any]]) -> int: ...

    async def mint_id(self) -> int: ...

    async def conversation(self, conversation_id: int) -> dict[str, Any] | None: ...

    async def scan_conversations(
        self, query: Mapping[str, Any], limit: int, cursor: Cursor | None = None
    ) -> Page: ...

    async def entry(self, conversation_id: int, entry_id: int) -> Mapping[str, Any] | None: ...

    async def entry_by_id(self, entry_id: int) -> dict[str, Any] | None: ...

    async def find_latest_head_marker(
        self, conversation_id: int, at_or_before: int | None = None
    ) -> Mapping[str, Any] | None: ...

    async def scan_entries(
        self, query: Mapping[str, Any], limit: int, cursor: Cursor | None = None
    ) -> Page: ...

    async def task(self, task_id: int) -> dict[str, Any] | None: ...

    async def scan_tasks(
        self, query: Mapping[str, Any], limit: int, cursor: Cursor | None = None
    ) -> Page: ...

    async def submission(self, submission_id: int) -> dict[str, Any] | None: ...

    async def scan_submissions(
        self, query: Mapping[str, Any], limit: int, cursor: Cursor | None = None
    ) -> Page: ...

    async def submission_by_request(self, conversation_id: int, request_id: str) -> dict[str, Any] | None: ...

    async def find_document(self, address: Mapping[str, Any], at: int | str) -> dict[str, Any] | None: ...

    async def document(self, document_id: int, at: int | str) -> Mapping[str, Any] | None: ...

    async def scan_documents(
        self, query: Mapping[str, Any], limit: int, cursor: Cursor | None = None
    ) -> Page: ...

    async def close(self) -> None: ...


def scan_start(requested: str | None, cursor: Cursor | None, fallback: ScanOrder) -> tuple[ScanOrder, int | None]:
    if requested is not None and requested not in ("ascending", "descending"):
        raise StorageRequestError(f"Invalid scan order: {requested!r}")
    if cursor is None:
        next_order: ScanOrder = requested if requested is not None else fallback  # type: ignore[assignment]
        return next_order, None
    after = cursor.get("after")
    stored = cursor.get("order")
    if not isinstance(after, int) or isinstance(after, bool):
        raise StorageRequestError("Invalid storage cursor")
    if stored is not None and stored not in ("ascending", "descending"):
        raise StorageRequestError("Invalid storage cursor")
    order: ScanOrder = stored or fallback
    if requested is not None and requested != order:
        raise StorageRequestError(f"The cursor continues a {order} scan; the query asks for {requested}")
    return order, after


def scan_indexes(ids: Sequence[int], order: ScanOrder, after: int | None) -> Iterable[int]:
    """Indexes of sorted IDs in scan order, after the cursor's ID when there is one."""
    import bisect

    if order == "ascending":
        start = 0 if after is None else bisect.bisect_right(ids, after)
        return range(start, len(ids))
    end = len(ids) if after is None else bisect.bisect_left(ids, after)
    return range(end - 1, -1, -1)


def page(values: Sequence[Any], limit: int, order: ScanOrder = "ascending") -> Page:
    if limit < 0:
        raise StorageRequestError("limit must not be negative")
    items = list(values[:limit])
    if len(values) <= limit:
        return make_page(items)
    return make_page(items, next_cursor(items[-1]["id"], order))


def check_limit(limit: int) -> None:
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 0:
        raise StorageRequestError("limit must be a non-negative integer")


def clone(value: Any) -> Any:
    return copy_json(value)


def document_version(revisions: Sequence[Mapping[str, Any]]) -> int:
    if not revisions:
        raise StorageRequestError("Document has no content")
    base = revisions[-1]
    return int(base["version"])


def materialize_revisions(
    document_id: int, record: Mapping[str, Any], revisions: Sequence[Mapping[str, Any]], at: int | str
) -> dict[str, Any] | None:
    from ..delta import apply_ops

    if at != "current" and is_current_only(record):
        raise StorageRequestError(f"Document {document_id} does not retain historical content")
    if not is_alive_at(record, at):
        return None
    usable = [revision for revision in revisions if at == "current" or revision["seq"] <= at]
    base_index = len(usable) - 1
    while base_index >= 0 and usable[base_index]["kind"] != "base":
        base_index -= 1
    if base_index < 0:
        raise StorageRequestError(f"Document {document_id} is missing a required base")
    base = usable[base_index]
    value = copy_json(base["value"])
    version = int(base["version"])
    for revision in usable[base_index + 1 :]:
        if revision["kind"] != "delta":
            raise StorageRequestError(f"Document {document_id} crosses a stored version boundary without a base")
        if int(revision["version"]) != version:
            raise StorageRequestError(f"Document {document_id} crosses a stored version boundary without a base")
        value = apply_ops(value, revision["ops"])
    return {
        "record": clone(record),
        "version": version,
        "value": value,
        "deltasSinceBase": len(usable) - base_index - 1,
    }


def record_address_key(record: Mapping[str, Any]) -> str:
    address: dict[str, Any] = {"kind": record["kind"], "scope": record["scope"]}
    if "key" in record:
        address["key"] = record["key"]
    return address_id(address)


def scope_key(scope: Mapping[str, Any]) -> str:
    kind = scope["kind"]
    if kind == "conversation":
        return f"conversation:{scope['conversationId']}"
    if kind == "task":
        return f"task:{scope['taskId']}"
    return "session"


__all__ = [
    "Storage",
    "check_limit",
    "clone",
    "document_version",
    "materialize_revisions",
    "page",
    "record_address_key",
    "scan_indexes",
    "scan_start",
    "scope_key",
]
