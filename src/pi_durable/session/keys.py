"""The keys of the Session line: what a commit writes and a job reads.

A job reads through :class:`GatedStorage`, which waits at the first read of a key an admitted,
unpublished commit wrote; the keys a commit writes come from its own settles
(:func:`table_keys` for the tables, :meth:`~pi_durable.session.transaction.Transaction.write_keys`
for documents). The scheduler's ownership keys -- the live work below an ownership node, the
ownerless roots, and the cancellation intent set (group-commit document §4.2) -- are not here:
the scheduler decides in recorded regions only once it does (#48), so nothing records them yet.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any, cast

from ..context import Context
from ..documents import address_id
from ..ids import ConversationId, DocumentId, EntryId, SubmissionId, TaskId
from ..types import (
    CommitReceipt,
    CommitResult,
    ConversationQuery,
    Cursor,
    DocumentAddress,
    DocumentPoint,
    DocumentQuery,
    DocumentRecord,
    DocumentScope,
    EntryQuery,
    Page,
    Storage,
    StorageWrite,
    SubmissionQuery,
    TaskQuery,
)

#: Every unpublished write: what a read that cannot be keyed, such as a scan, touches.
ALL_KEYS = "*"


def task_key(id: TaskId) -> str:  # noqa: A002 - the record's own identity
    """A task record."""

    return f"task:{id}"


def conversation_key(id: ConversationId) -> str:  # noqa: A002 - the record's own identity
    """A conversation record."""

    return f"conv:{id}"


def entry_key(id: EntryId) -> str:  # noqa: A002 - the record's own identity
    """An entry record."""

    return f"entry:{id}"


def append_key(id: ConversationId) -> str:  # noqa: A002 - the record's own identity
    """The newest entries of a conversation: appended blind, read by scans, the newest entry and markers."""

    return f"entry-append:{id}"


def submission_key(id: SubmissionId) -> str:  # noqa: A002 - the record's own identity
    """A submission record."""

    return f"sub:{id}"


def request_key(conversation_id: ConversationId, request_id: str) -> str:
    """The submission a conversation's request ID names."""

    return f"req:{conversation_id}:{request_id}"


def document_key(address: str) -> str:
    """A document address (``address_id()``): its current incarnation and value."""

    return f"doc:{address}"


def incarnation_key(id: DocumentId) -> str:  # noqa: A002 - the record's own identity
    """A document incarnation, for reads by its ID."""

    return f"docid:{id}"


def scope_key(scope: DocumentScope) -> str:
    """The incarnations alive in a scope: created and retired blind, read by scans."""

    kind = scope["kind"]
    if kind == "conversation":
        return f"docscope:c{cast(Any, scope)['conversationId']}"
    if kind == "task":
        return f"docscope:t{cast(Any, scope)['taskId']}"
    return "docscope:session"


def table_keys(writes: Sequence[StorageWrite], keys: list[str]) -> None:
    """Append the keys of the table records in ``writes``, the storage's own classification.

    A document write adds no key here: it is keyed by the transaction, where the address is known.
    A write of a type no storage stores adds nothing.
    """

    for write in writes:
        if write["type"] == "conversation":
            keys.append(conversation_key(write["value"]["id"]))
        elif write["type"] == "entry":
            keys.append(entry_key(write["value"]["id"]))
            keys.append(append_key(write["value"]["conversationId"]))
        elif write["type"] == "task":
            keys.append(task_key(write["value"]["id"]))
        elif write["type"] == "submission":
            keys.append(submission_key(write["value"]["id"]))
            # A submission write carries its request key with it: the lookup by request ID reads it.
            if write["value"].get("requestId") is not None:
                keys.append(request_key(write["value"]["conversationId"], write["value"]["requestId"]))


class GatedStorage:
    """The Storage a job on the line reads through.

    Every read first touches its key, which waits while that key has an unpublished writer, so the
    read sees what the line admitted before it. Scans that cannot be keyed touch
    :data:`ALL_KEYS`. Commits, :meth:`~pi_durable.types.Storage.mint_id` and ``close`` pass
    through: the line never commits through this view.
    """

    def __init__(self, storage: Storage, touch: Callable[[str], Awaitable[None] | None]) -> None:
        self._storage = storage
        self._touch = touch

    async def commit_batches(
        self,
        batches: Sequence[Sequence[StorageWrite]],
        receive: CommitReceipt,
        context: Context,
    ) -> CommitResult:
        return await self._storage.commit_batches(batches, receive, context)

    async def mint_id(self) -> int:
        return await self._storage.mint_id()

    async def conversation(self, id: ConversationId, context: Context) -> Any:
        return await self._after(conversation_key(id), lambda: self._storage.conversation(id, context))

    async def scan_conversations(
        self,
        query: ConversationQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[Any, Cursor]:
        return await self._after(
            ALL_KEYS, lambda: self._storage.scan_conversations(query, limit, cursor, context)
        )

    async def entry(
        self,
        *args: Any,
        context: Context | None = None,
        **kwargs: Any,
    ) -> Any:
        if context is None and len(args) == 3:
            conversation_id, id, resolved = cast(Any, args)
            # Visibility follows the conversation's immutable ancestry; it may be unpublished itself.
            return await self._after(
                conversation_key(conversation_id),
                lambda: self._after(
                    entry_key(id),
                    lambda: self._storage.entry(conversation_id, id, resolved),
                ),
            )
        if context is None and len(args) == 2 and isinstance(args[1], Context):
            id, resolved = cast(Any, args)
            return await self._after(entry_key(id), lambda: self._storage.entry(id, resolved))
        if context is None:
            raise TypeError("Storage.entry() requires a context")
        return await self._after(entry_key(args[0]), lambda: self._storage.entry(args[0], context))

    async def find_latest_head_marker(
        self,
        conversation_id: ConversationId,
        at_or_before_entry_id: EntryId | None,
        context: Context,
    ) -> Any:
        return await self._after_entries(
            conversation_id,
            lambda: self._storage.find_latest_head_marker(conversation_id, at_or_before_entry_id, context),
        )

    async def scan_entries(
        self,
        query: EntryQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[Any, Cursor]:
        return await self._after_entries(
            query["conversationId"],
            lambda: self._storage.scan_entries(query, limit, cursor, context),
        )

    async def task(self, id: TaskId, context: Context) -> Any:
        return await self._after(task_key(id), lambda: self._storage.task(id, context))

    async def scan_tasks(
        self,
        query: TaskQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[Any, Cursor]:
        return await self._after(ALL_KEYS, lambda: self._storage.scan_tasks(query, limit, cursor, context))

    async def submission(self, id: SubmissionId, context: Context) -> Any:
        return await self._after(submission_key(id), lambda: self._storage.submission(id, context))

    async def scan_submissions(
        self,
        query: SubmissionQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[Any, Cursor]:
        return await self._after(
            ALL_KEYS, lambda: self._storage.scan_submissions(query, limit, cursor, context)
        )

    async def submission_by_request(
        self, conversation_id: ConversationId, request_id: str, context: Context
    ) -> Any:
        return await self._after(
            request_key(conversation_id, request_id),
            lambda: self._storage.submission_by_request(conversation_id, request_id, context),
        )

    async def find_document(
        self, address: DocumentAddress, at: DocumentPoint, context: Context
    ) -> DocumentRecord | None:
        return await self._after(
            document_key(address_id(address)),
            lambda: self._storage.find_document(address, at, context),
        )

    async def document(self, id: DocumentId, at: DocumentPoint, context: Context) -> Any:
        return await self._after(incarnation_key(id), lambda: self._storage.document(id, at, context))

    async def scan_documents(
        self,
        query: DocumentQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[DocumentRecord, Cursor]:
        return await self._after(
            scope_key(query["scope"]),
            lambda: self._storage.scan_documents(query, limit, cursor, context),
        )

    async def close(self, context: Context) -> None:
        await self._storage.close(context)

    async def _after_entries(self, conversation_id: ConversationId, read: Callable[[], Awaitable[Any]]):
        """Read a conversation's entries once neither its appends nor its creation is unpublished.

        Storage refuses the read of a conversation it does not have yet.
        """

        return await self._after(
            conversation_key(conversation_id),
            lambda: self._after(append_key(conversation_id), read),
        )

    async def _after(self, key: str, read: Callable[[], Awaitable[Any]]) -> Any:
        gate = self._touch(key)
        if gate is not None:
            await gate
        return await read()


__all__ = [
    "ALL_KEYS",
    "GatedStorage",
    "append_key",
    "conversation_key",
    "document_key",
    "entry_key",
    "incarnation_key",
    "request_key",
    "scope_key",
    "submission_key",
    "table_keys",
    "task_key",
]
