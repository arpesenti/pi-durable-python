import inspect
from collections.abc import Sequence
from typing import get_overloads

from pi_durable.context import BACKGROUND_CONTEXT, Context
from pi_durable.ids import (
    ConversationId,
    DocumentId,
    EntryId,
    Seq,
    SubmissionId,
    TaskId,
)
from pi_durable.types import (
    ConversationQuery,
    ConversationRecord,
    Cursor,
    DocumentAddress,
    DocumentPoint,
    DocumentQuery,
    DocumentRecord,
    EntryLookup,
    EntryQuery,
    EntryRecord,
    EntryWithHead,
    Page,
    Storage,
    StorageWrite,
    StoredDocument,
    SubmissionQuery,
    SubmissionRecord,
    TaskQuery,
    TaskRecord,
)


class FakeStorage:
    def __init__(self) -> None:
        self.closed = False

    async def commit(self, writes: Sequence[StorageWrite], context: Context) -> Seq:
        assert context is BACKGROUND_CONTEXT
        return Seq(1)

    async def mint_id(self) -> ConversationId:
        return ConversationId(2)

    async def conversation(self, id: ConversationId, context: Context) -> ConversationRecord | None:
        return None

    async def scan_conversations(
        self,
        query: ConversationQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[ConversationRecord, Cursor]:
        return {"items": []}

    async def entry(self, *args: object, **kwargs: object) -> EntryLookup | None:
        return None

    async def find_latest_head_marker(
        self,
        conversation_id: ConversationId,
        at_or_before_entry_id: EntryId | None,
        context: Context,
    ) -> EntryWithHead | None:
        return None

    async def scan_entries(
        self,
        query: EntryQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[EntryRecord, Cursor]:
        return {"items": []}

    async def task(self, id: TaskId, context: Context) -> TaskRecord | None:
        return None

    async def scan_tasks(
        self,
        query: TaskQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[TaskRecord, Cursor]:
        return {"items": []}

    async def submission(self, id: SubmissionId, context: Context) -> SubmissionRecord | None:
        return None

    async def scan_submissions(
        self,
        query: SubmissionQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[SubmissionRecord, Cursor]:
        return {"items": []}

    async def submission_by_request(
        self,
        conversation_id: ConversationId,
        request_id: str,
        context: Context,
    ) -> SubmissionRecord | None:
        return None

    async def find_document(
        self,
        address: DocumentAddress,
        at: DocumentPoint,
        context: Context,
    ) -> DocumentRecord | None:
        return None

    async def document(
        self,
        id: DocumentId,
        at: DocumentPoint,
        context: Context,
    ) -> StoredDocument | None:
        return None

    async def scan_documents(
        self,
        query: DocumentQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[DocumentRecord, Cursor]:
        return {"items": []}

    async def close(self, context: Context) -> None:
        self.closed = True


def test_storage_protocol_is_structural() -> None:
    assert isinstance(FakeStorage(), Storage)


def test_storage_protocol_matches_the_reference_operation_names() -> None:
    assert {
        "commit",
        "mint_id",
        "conversation",
        "scan_conversations",
        "entry",
        "find_latest_head_marker",
        "scan_entries",
        "task",
        "scan_tasks",
        "submission",
        "scan_submissions",
        "submission_by_request",
        "find_document",
        "document",
        "scan_documents",
        "close",
    } <= set(Storage.__dict__)


def test_every_storage_operation_except_minting_takes_a_context() -> None:
    for name, member in Storage.__dict__.items():
        if name.startswith("_") or not callable(member) or name == "mint_id":
            continue
        parameters = inspect.signature(member).parameters
        assert "context" in parameters, name


def test_both_entry_overloads_take_a_context() -> None:
    overloads = get_overloads(Storage.entry)
    assert len(overloads) == 2
    for overload in overloads:
        assert "context" in inspect.signature(overload).parameters


async def test_minimal_storage_operations_round_trip() -> None:
    storage = FakeStorage()
    assert isinstance(storage, Storage)
    assert await storage.mint_id() == 2
    assert await storage.commit([], BACKGROUND_CONTEXT) == 1
    assert await storage.conversation(ConversationId(1), BACKGROUND_CONTEXT) is None
    assert await storage.entry(EntryId(2), BACKGROUND_CONTEXT) is None
    assert await storage.entry(ConversationId(1), EntryId(2), BACKGROUND_CONTEXT) is None
    assert await storage.find_latest_head_marker(ConversationId(1), None, BACKGROUND_CONTEXT) is None
    page = await storage.scan_entries(
        {"conversationId": ConversationId(1)}, 10, None, BACKGROUND_CONTEXT
    )
    assert page == {"items": []}
    await storage.close(BACKGROUND_CONTEXT)
    assert storage.closed is True
