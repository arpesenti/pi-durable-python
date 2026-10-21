"""Core durable records, queries, and the Storage protocol.

Records are persisted JSON objects with camelCase fields, exactly as in the reference package.
Python type declarations are ``TypedDict`` values so storage, JSONL, and SQLite carry the same
shape without a mapping layer. This module deliberately does not use postponed annotations:
``TypedDict`` computes ``__required_keys__``/``__optional_keys__`` at class creation, and PEP 563
string annotations would make every field appear required at runtime.

Entry records are the immutable value of the transcript. :func:`entry_draft` validates a draft
and :func:`entry_record` assigns its identity, resolving ``head: "self"``; both return deep-frozen
JSON values that cannot be mutated after construction.
"""

import asyncio
from collections.abc import Callable, Mapping, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    Never,
    NotRequired,
    Protocol,
    TypeAlias,
    TypedDict,
    TypeVar,
    cast,
    overload,
    runtime_checkable,
)

from .context import Context
from .ids import (
    ROOT_CONVERSATION_ID,
    ConversationId,
    DocumentId,
    EntryId,
    Id,
    Seq,
    SubmissionId,
    TaskId,
    require_positive_int,
)
from .json import JsonObject, JsonValue, copy_json, freeze_json

if TYPE_CHECKING:
    from .harness.types import ContextView

__all__ = [
    "ROOT_CONVERSATION_ID",
    "CheckpointInfo",
    "CommitChange",
    "CommitPublication",
    "ContextEdit",
    "ConversationId",
    "ConversationOwnership",
    "ConversationQuery",
    "ConversationRecord",
    "Cursor",
    "DocumentAddress",
    "DocumentCommitChange",
    "DocumentContent",
    "DocumentCopySource",
    "DocumentCreate",
    "DocumentId",
    "DocumentPoint",
    "DocumentQuery",
    "DocumentRecord",
    "DocumentObserver",
    "DocumentSemantics",
    "DocumentWatch",
    "EntryDraft",
    "EntryId",
    "EntryQuery",
    "EntryRecord",
    "EntryWithHead",
    "Id",
    "JoinPolicy",
    "JsonObject",
    "JsonValue",
    "Page",
    "RunningTask",
    "ScanOrder",
    "Seq",
    "Session",
    "Storage",
    "StorageWrite",
    "StoredDocument",
    "SubmissionCreate",
    "SubmissionId",
    "SubmissionQuery",
    "SubmissionRecord",
    "SubmissionSettlement",
    "TableCommitChange",
    "TaskId",
    "TaskOptions",
    "TaskOutcome",
    "TaskOutcomeError",
    "TaskOwnership",
    "TaskQuery",
    "TaskRecord",
    "TaskState",
    "Tx",
    "TypedEntry",
    "TypedEntryDraft",
    "WatchEnd",
    "WatchEnded",
    "WatchHandle",
    "entry_draft",
    "entry_record",
]

MessageRecord: TypeAlias = dict[str, JsonValue]
"""One model-facing message. The pi-ai adapter supplies its concrete shape."""
Op: TypeAlias = list[Any]
"""One Chord delta operation, represented as its JSON array."""
JsonValueT_co = TypeVar("JsonValueT_co", covariant=True)


# ── Identity, conversation, and entry records ──────────────────────────────


class ConversationParent(TypedDict):
    conversationId: ConversationId
    at: EntryId


class ConversationOwner(TypedDict):
    conversationId: ConversationId
    taskId: TaskId


class ConversationRecord(TypedDict):
    id: ConversationId
    parent: NotRequired[ConversationParent]
    owner: NotRequired[ConversationOwner]


class OwnerlessConversation(TypedDict):
    kind: Literal["ownerless"]


class TaskOwnedConversation(TypedDict):
    kind: Literal["task"]
    taskId: TaskId


ConversationOwnership: TypeAlias = OwnerlessConversation | TaskOwnedConversation


class ConversationOwnedTask(TypedDict):
    kind: Literal["conversation"]


TaskOwnership: TypeAlias = ConversationOwnedTask | TaskOwnedConversation


class OmitContextEdit(TypedDict):
    target: EntryId
    action: Literal["omit"]
    messages: NotRequired[Never]


class ReplaceContextEdit(TypedDict):
    target: EntryId
    action: Literal["replace"]
    messages: list[MessageRecord]


ContextEdit: TypeAlias = OmitContextEdit | ReplaceContextEdit


class EntryRecord(TypedDict):
    id: EntryId
    conversationId: ConversationId
    kind: str
    model: NotRequired[list[MessageRecord]]
    data: NotRequired[JsonValue]
    head: NotRequired[EntryId]
    edits: NotRequired[list[ContextEdit]]
    byTaskId: NotRequired[TaskId]


class EntryDraft(TypedDict):
    kind: str
    model: NotRequired[list[MessageRecord]]
    data: NotRequired[JsonValue]
    head: NotRequired[EntryId | Literal["self"]]
    edits: NotRequired[list[ContextEdit]]


class EntryWithHead(TypedDict):
    """An entry that is also a head marker, so its ``head`` is the range's lower bound."""

    id: EntryId
    conversationId: ConversationId
    kind: str
    model: NotRequired[list[MessageRecord]]
    data: NotRequired[JsonValue]
    head: EntryId
    edits: NotRequired[list[ContextEdit]]
    byTaskId: NotRequired[TaskId]


D = TypeVar("D", bound=JsonValue)


class TypedEntry(TypedDict, Generic[D]):
    """An entry record whose ``data`` is narrowed to ``D`` when present."""

    id: EntryId
    conversationId: ConversationId
    kind: str
    model: NotRequired[list[MessageRecord]]
    data: NotRequired[D]
    head: NotRequired[EntryId]
    edits: NotRequired[list[ContextEdit]]
    byTaskId: NotRequired[TaskId]


class TypedEntryDraft(TypedDict, Generic[D]):
    """Entry draft content of a typed kind; the token supplies ``kind``."""

    model: NotRequired[list[MessageRecord]]
    data: NotRequired[D]
    head: NotRequired[EntryId | Literal["self"]]
    edits: NotRequired[list[ContextEdit]]


# ── Submission records ──────────────────────────────────────────────────────


class SubmissionRecordBase(TypedDict):
    id: SubmissionId
    conversationId: ConversationId
    requestId: NotRequired[str]


class InputQueuedRecord(SubmissionRecordBase):
    type: Literal["input"]
    status: Literal["queued"]
    entry: NotRequired[Never]
    answer: NotRequired[Never]
    reason: NotRequired[Never]
    detail: NotRequired[Never]


class InputPlacedRecord(SubmissionRecordBase):
    type: Literal["input"]
    status: Literal["placed"]
    entry: EntryId
    answer: NotRequired[Never]
    reason: NotRequired[Never]
    detail: NotRequired[Never]


class InputDoneRecord(SubmissionRecordBase):
    type: Literal["input"]
    status: Literal["done"]
    entry: EntryId
    answer: EntryId
    reason: NotRequired[Never]
    detail: NotRequired[Never]


class InputUnansweredRecord(SubmissionRecordBase):
    type: Literal["input"]
    status: Literal["unanswered"]
    entry: NotRequired[EntryId]
    reason: str
    detail: NotRequired[JsonValue]
    answer: NotRequired[Never]


class WriteQueuedRecord(SubmissionRecordBase):
    type: Literal["write"]
    status: Literal["queued"]
    entry: NotRequired[Never]
    answer: NotRequired[Never]
    reason: NotRequired[Never]
    detail: NotRequired[Never]


class WriteDoneRecord(SubmissionRecordBase):
    type: Literal["write"]
    status: Literal["done"]
    entry: EntryId
    answer: NotRequired[Never]
    reason: NotRequired[Never]
    detail: NotRequired[Never]


class WriteUnansweredRecord(SubmissionRecordBase):
    type: Literal["write"]
    status: Literal["unanswered"]
    reason: str
    detail: NotRequired[JsonValue]
    entry: NotRequired[Never]
    answer: NotRequired[Never]


SubmissionRecord: TypeAlias = (
    InputQueuedRecord
    | InputPlacedRecord
    | InputDoneRecord
    | InputUnansweredRecord
    | WriteQueuedRecord
    | WriteDoneRecord
    | WriteUnansweredRecord
)


class SubmissionCreateBase(TypedDict):
    conversationId: ConversationId
    requestId: NotRequired[str]
    id: NotRequired[Never]


class InputQueuedCreate(SubmissionCreateBase):
    type: Literal["input"]
    status: Literal["queued"]
    entry: NotRequired[Never]
    answer: NotRequired[Never]
    reason: NotRequired[Never]
    detail: NotRequired[Never]


class InputPlacedCreate(SubmissionCreateBase):
    type: Literal["input"]
    status: Literal["placed"]
    entry: EntryId
    answer: NotRequired[Never]
    reason: NotRequired[Never]
    detail: NotRequired[Never]


class InputDoneCreate(SubmissionCreateBase):
    type: Literal["input"]
    status: Literal["done"]
    entry: EntryId
    answer: EntryId
    reason: NotRequired[Never]
    detail: NotRequired[Never]


class InputUnansweredCreate(SubmissionCreateBase):
    type: Literal["input"]
    status: Literal["unanswered"]
    entry: NotRequired[EntryId]
    reason: str
    detail: NotRequired[JsonValue]
    answer: NotRequired[Never]


class WriteQueuedCreate(SubmissionCreateBase):
    type: Literal["write"]
    status: Literal["queued"]
    entry: NotRequired[Never]
    answer: NotRequired[Never]
    reason: NotRequired[Never]
    detail: NotRequired[Never]


class WriteDoneCreate(SubmissionCreateBase):
    type: Literal["write"]
    status: Literal["done"]
    entry: EntryId
    answer: NotRequired[Never]
    reason: NotRequired[Never]
    detail: NotRequired[Never]


class WriteUnansweredCreate(SubmissionCreateBase):
    type: Literal["write"]
    status: Literal["unanswered"]
    reason: str
    detail: NotRequired[JsonValue]
    entry: NotRequired[Never]
    answer: NotRequired[Never]


SubmissionCreate: TypeAlias = (
    InputQueuedCreate
    | InputPlacedCreate
    | InputDoneCreate
    | InputUnansweredCreate
    | WriteQueuedCreate
    | WriteDoneCreate
    | WriteUnansweredCreate
)


class DoneSubmissionSettlement(TypedDict):
    status: Literal["done"]
    answer: EntryId


class UnansweredSubmissionSettlement(TypedDict):
    status: Literal["unanswered"]
    reason: str
    detail: NotRequired[JsonValue]
    answer: NotRequired[Never]


SubmissionSettlement: TypeAlias = DoneSubmissionSettlement | UnansweredSubmissionSettlement


# ── Task records ────────────────────────────────────────────────────────────

JoinPolicy: TypeAlias = Literal["failFast", "allSettled"]


class TaskOutcomeError(TypedDict):
    message: str
    detail: NotRequired[JsonValue]


R = TypeVar("R")


class TaskOutcomeCompleted(TypedDict, Generic[R]):
    status: Literal["completed"]
    result: R
    error: NotRequired[Never]
    reason: NotRequired[Never]


class TaskOutcomeFailed(TypedDict, Generic[R]):
    status: Literal["failed"]
    error: TaskOutcomeError
    result: NotRequired[R]
    reason: NotRequired[Never]


class TaskOutcomeAborted(TypedDict, Generic[R]):
    status: Literal["aborted"]
    reason: NotRequired[str]
    result: NotRequired[R]
    error: NotRequired[Never]


class TaskOutcomeOrphaned(TypedDict):
    status: Literal["orphaned"]
    reason: str
    result: NotRequired[Never]
    error: NotRequired[Never]


class TaskOutcomeFaulted(TypedDict):
    status: Literal["faulted"]
    error: TaskOutcomeError
    result: NotRequired[Never]
    reason: NotRequired[Never]


TaskOutcome: TypeAlias = (
    TaskOutcomeCompleted[Any]
    | TaskOutcomeFailed[Any]
    | TaskOutcomeAborted[Any]
    | TaskOutcomeOrphaned
    | TaskOutcomeFaulted
)

S = TypeVar("S")


class TaskStatePending(TypedDict, Generic[S]):
    status: Literal["pending"]
    checkpoint: S
    outcome: NotRequired[Never]


class TaskStateRunning(TypedDict, Generic[S]):
    status: Literal["running"]
    checkpoint: S
    outcome: NotRequired[Never]


class TaskStateWaiting(TypedDict, Generic[S]):
    status: Literal["waiting"]
    checkpoint: S
    on: list[TaskId]
    policy: JoinPolicy
    outcome: NotRequired[Never]


class TaskStateCompleting(TypedDict, Generic[S]):
    status: Literal["completing"]
    outcome: TaskOutcome
    checkpoint: NotRequired[Never]


class TaskStateTerminal(TypedDict, Generic[S]):
    status: Literal["terminal"]
    outcome: TaskOutcome
    checkpoint: NotRequired[Never]


TaskState: TypeAlias = (
    TaskStatePending[Any]
    | TaskStateRunning[Any]
    | TaskStateWaiting[Any]
    | TaskStateCompleting[Any]
    | TaskStateTerminal[Any]
)
NextTaskState: TypeAlias = TaskStateRunning[Any] | TaskStateWaiting[Any] | TaskStateTerminal[Any]


class TaskRecordBase(TypedDict):
    id: TaskId
    conversationId: ConversationId
    kind: str
    version: int
    input: JsonValue
    owner: NotRequired[TaskId]
    background: bool
    abortRequested: bool
    abortReason: NotRequired[Literal["restart"]]
    abandonOnRestart: NotRequired[Literal[True]]
    startedAt: NotRequired[int]
    endedAt: NotRequired[int]


class LiveTaskRecord(TaskRecordBase):
    state: TaskStatePending[Any] | TaskStateRunning[Any] | TaskStateWaiting[Any]
    memos: NotRequired[dict[str, JsonValue]]


class SettledTaskRecord(TaskRecordBase):
    state: TaskStateCompleting[Any] | TaskStateTerminal[Any]
    memos: NotRequired[Never]


TaskRecord: TypeAlias = LiveTaskRecord | SettledTaskRecord


class RunningTaskRecord(TaskRecordBase):
    state: TaskStateRunning[Any]
    memos: NotRequired[dict[str, JsonValue]]


RunningTask: TypeAlias = RunningTaskRecord


class TaskOptions(TypedDict):
    ownership: TaskOwnership
    conversationId: NotRequired[ConversationId]
    background: NotRequired[bool]
    abandonOnRestart: NotRequired[bool]


# ── Documents ───────────────────────────────────────────────────────────────


class SessionDocumentSemantics(TypedDict):
    scope: Literal["session"]


class LatestConversationSemantics(TypedDict):
    scope: Literal["conversation"]
    history: Literal["latest"]
    fork: Literal["current", "initial"]


class RewindableConversationSemantics(TypedDict):
    scope: Literal["conversation"]
    history: Literal["rewindable"]
    fork: Literal["asOf", "current", "initial"]


class TaskDocumentSemantics(TypedDict):
    scope: Literal["task"]


DocumentSemantics: TypeAlias = (
    SessionDocumentSemantics
    | LatestConversationSemantics
    | RewindableConversationSemantics
    | TaskDocumentSemantics
)


class CheckpointInfo(TypedDict):
    deltasSinceBase: int


class SessionDocumentScope(TypedDict):
    kind: Literal["session"]


class ConversationDocumentScope(TypedDict):
    kind: Literal["conversation"]
    conversationId: ConversationId


class TaskDocumentScope(TypedDict):
    kind: Literal["task"]
    taskId: TaskId


DocumentScope: TypeAlias = SessionDocumentScope | ConversationDocumentScope | TaskDocumentScope


class DocumentRecordBase(TypedDict):
    id: DocumentId
    kind: str
    key: NotRequired[str]
    createdAt: Seq
    retiredAt: NotRequired[Seq]


class SessionDocumentRecord(DocumentRecordBase):
    scope: SessionDocumentScope
    history: NotRequired[Never]
    fork: NotRequired[Never]


class LatestConversationDocumentRecord(DocumentRecordBase):
    scope: ConversationDocumentScope
    history: Literal["latest"]
    fork: Literal["current", "initial"]


class RewindableConversationDocumentRecord(DocumentRecordBase):
    scope: ConversationDocumentScope
    history: Literal["rewindable"]
    fork: Literal["asOf", "current", "initial"]


class TaskDocumentRecord(DocumentRecordBase):
    scope: TaskDocumentScope
    history: NotRequired[Never]
    fork: NotRequired[Never]


DocumentRecord: TypeAlias = (
    SessionDocumentRecord
    | LatestConversationDocumentRecord
    | RewindableConversationDocumentRecord
    | TaskDocumentRecord
)


class DocumentCreateBase(TypedDict):
    id: DocumentId
    kind: str
    key: NotRequired[str]
    createdAt: NotRequired[Never]
    retiredAt: NotRequired[Never]


class SessionDocumentCreate(DocumentCreateBase):
    scope: SessionDocumentScope
    history: NotRequired[Never]
    fork: NotRequired[Never]


class LatestConversationDocumentCreate(DocumentCreateBase):
    scope: ConversationDocumentScope
    history: Literal["latest"]
    fork: Literal["current", "initial"]


class RewindableConversationDocumentCreate(DocumentCreateBase):
    scope: ConversationDocumentScope
    history: Literal["rewindable"]
    fork: Literal["asOf", "current", "initial"]


class TaskDocumentCreate(DocumentCreateBase):
    scope: TaskDocumentScope
    history: NotRequired[Never]
    fork: NotRequired[Never]


DocumentCreate: TypeAlias = (
    SessionDocumentCreate
    | LatestConversationDocumentCreate
    | RewindableConversationDocumentCreate
    | TaskDocumentCreate
)


class DocumentBaseContent(TypedDict):
    version: int
    kind: Literal["base"]
    value: JsonObject
    ops: NotRequired[Never]


class DocumentDeltaContent(TypedDict):
    version: int
    kind: Literal["delta"]
    ops: list[Op]
    value: NotRequired[Never]


DocumentContent: TypeAlias = DocumentBaseContent | DocumentDeltaContent
DocumentPoint: TypeAlias = Seq | Literal["current"]


class DocumentCopySource(TypedDict):
    id: DocumentId
    at: DocumentPoint


class StoredDocument(TypedDict):
    record: DocumentRecord
    version: int
    value: JsonObject
    deltasSinceBase: int


class DocumentAddress(TypedDict):
    kind: str
    scope: DocumentScope
    key: NotRequired[str]


class DocumentQuery(TypedDict):
    scope: DocumentScope
    at: DocumentPoint
    kind: NotRequired[str]


# ── Commits and storage writes ──────────────────────────────────────────────


class ConversationWrite(TypedDict):
    type: Literal["conversation"]
    value: ConversationRecord


class EntryWrite(TypedDict):
    type: Literal["entry"]
    value: EntryRecord


class TaskWrite(TypedDict):
    type: Literal["task"]
    value: TaskRecord


class SubmissionWrite(TypedDict):
    type: Literal["submission"]
    value: SubmissionRecord


class DocumentCreateWrite(TypedDict):
    type: Literal["document.create"]
    record: DocumentCreate
    content: DocumentBaseContent


class DocumentCopyWrite(TypedDict):
    type: Literal["document.copy"]
    record: DocumentCreate
    source: DocumentCopySource


class DocumentChangeWrite(TypedDict):
    type: Literal["document.change"]
    id: DocumentId
    content: DocumentContent


class DocumentRetireWrite(TypedDict):
    type: Literal["document.retire"]
    id: DocumentId


StorageWrite: TypeAlias = (
    ConversationWrite
    | EntryWrite
    | TaskWrite
    | SubmissionWrite
    | DocumentCreateWrite
    | DocumentCopyWrite
    | DocumentChangeWrite
    | DocumentRetireWrite
)

TableCommitChange: TypeAlias = ConversationWrite | EntryWrite | TaskWrite | SubmissionWrite


class DocumentCommittedChange(TypedDict):
    type: Literal["document"]
    record: DocumentRecord
    conversationId: ConversationId | None
    version: int | None
    value: JsonObject | None
    ops: list[Op]


class DocumentCopiedChange(TypedDict):
    type: Literal["document.copy"]
    record: DocumentRecord
    conversationId: ConversationId
    source: DocumentCopySource


DocumentCommitChange: TypeAlias = DocumentCommittedChange | DocumentCopiedChange
CommitChange: TypeAlias = TableCommitChange | DocumentCommitChange


class CommitPublication(TypedDict):
    seq: Seq
    changes: list[CommitChange]


class WatchEnded(TypedDict):
    reason: Literal["stopped", "cancelled", "session_closed", "retired"]


class WatchListenerErrorEnd(TypedDict):
    reason: Literal["listener_error"]
    error: BaseException


class WatchSessionFailedEnd(TypedDict):
    reason: Literal["session_failed"]
    error: object


WatchEnd: TypeAlias = WatchEnded | WatchListenerErrorEnd | WatchSessionFailedEnd
"""Terminal result of one document watch."""


class WatchHandle(Protocol, Generic[JsonValueT_co]):
    """Serialized exact-frame observation of an immutable value with bounded pending delivery."""

    @property
    def value(self) -> JsonValueT_co:
        """Acquisition revision before start; latest delivered immutable revision afterward."""
        ...

    @property
    def closed(self) -> asyncio.Future[WatchEnd]:
        """Settle when the watch terminates; an already-running callback remains caller-owned."""
        ...

    def start(self, listener: Callable[[JsonValueT_co, list[Op], Context], Any]) -> None:
        """Install the sole asynchronous listener. Never invokes it inline."""
        ...

    def stop(self) -> asyncio.Future[WatchEnd]:
        """Idempotently stop future callbacks and return this watch's terminal result."""
        ...


DocumentWatch: TypeAlias = WatchHandle[JsonObject | None]
"""Serialized exact-frame observation of one document incarnation."""


class DocumentObserver(Protocol):
    """Non-creating document watch acquisition shared by Session and later invocation APIs."""

    async def watch_doc(self, token: Any, *args: Any) -> DocumentWatch | None: ...


# ── Queries and pages ───────────────────────────────────────────────────────

ScanOrder: TypeAlias = Literal["ascending", "descending"]
Cursor: TypeAlias = dict[str, JsonValue]

ItemT = TypeVar("ItemT")
CursorT = TypeVar("CursorT")


class Page(TypedDict, Generic[ItemT, CursorT]):
    items: list[ItemT]
    next: NotRequired[CursorT]


class ConversationQuery(TypedDict):
    ownerConversationId: NotRequired[ConversationId]
    ownerTaskId: NotRequired[TaskId]
    order: NotRequired[ScanOrder]


class EntryQuery(TypedDict):
    conversationId: ConversationId
    minEntryId: NotRequired[EntryId]
    maxEntryId: NotRequired[EntryId]
    order: NotRequired[ScanOrder]


class TaskQuery(TypedDict):
    conversationId: NotRequired[ConversationId]
    kind: NotRequired[str]
    status: NotRequired[Literal["pending", "running", "waiting", "completing", "terminal"]]
    abortRequested: NotRequired[bool]
    background: NotRequired[bool]
    order: NotRequired[ScanOrder]


class SubmissionQuery(TypedDict):
    conversationId: NotRequired[ConversationId]
    status: NotRequired[Literal["queued", "placed", "done", "unanswered"]]
    order: NotRequired[ScanOrder]


class EntryLookup(TypedDict):
    entry: EntryRecord
    commitSeq: Seq


# ── Storage protocol ────────────────────────────────────────────────────────


@runtime_checkable
class Storage(Protocol):
    """Atomic persistence boundary for Session records.

    Storage trusts the owning Session to supply semantically valid records, references, ancestry,
    and transitions. Implementations enforce atomicity, global ID ownership, immutable
    conversation/entry creation, document record consistency, and detached values; the Session
    serializes commits.

    An error a method throws is final: it fails the Session, which nothing retries, so retry
    transient failures inside the method. Two errors of a read fail only that read: a
    :class:`~pi_durable.errors.StorageRequestError` for an invalid request, and a rejection
    because its caller's context was aborted. Any error from :meth:`commit` or :meth:`mint_id`
    is fatal.
    """

    async def commit(self, writes: Sequence[StorageWrite], context: Context) -> Seq:
        """Atomically persist one batch and return its sequence."""

    async def mint_id(self) -> int:
        """Return a fresh ID greater than every ID minted or stored before, also across reopen.

        The value is unbranded; a caller applies :func:`~pi_durable.ids.id_from_number` at its
        own trusted boundary.
        """

    async def conversation(self, id: ConversationId, context: Context) -> ConversationRecord | None:
        """Look up one conversation by exact ID."""

    async def scan_conversations(
        self,
        query: ConversationQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[ConversationRecord, Cursor]:
        """Scan conversations in ``query['order']`` (default ascending) by ID."""

    @overload
    async def entry(self, id: EntryId, context: Context) -> EntryLookup | None: ...

    @overload
    async def entry(
        self,
        conversation_id: ConversationId,
        id: EntryId,
        context: Context,
    ) -> EntryLookup | None: ...

    async def entry(
        self,
        *args: Any,
        context: Context | None = None,
        **kwargs: Any,
    ) -> EntryLookup | None:
        """Look up one global entry, or one visible through a conversation's ancestry."""

    async def find_latest_head_marker(
        self,
        conversation_id: ConversationId,
        at_or_before_entry_id: EntryId | None,
        context: Context,
    ) -> EntryWithHead | None:
        """Return the newest visible entry with a ``head`` at or below the optional cutoff."""

    async def scan_entries(
        self,
        query: EntryQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[EntryRecord, Cursor]:
        """Scan the inclusive visible range in ``query['order']`` (default descending)."""

    async def task(self, id: TaskId, context: Context) -> TaskRecord | None:
        """Look up the latest complete record for one task."""

    async def scan_tasks(
        self,
        query: TaskQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[TaskRecord, Cursor]:
        """Scan task records matching every supplied filter."""

    async def submission(self, id: SubmissionId, context: Context) -> SubmissionRecord | None:
        """Look up the latest complete record for one admitted submission."""

    async def scan_submissions(
        self,
        query: SubmissionQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[SubmissionRecord, Cursor]:
        """Scan submissions matching every supplied filter."""

    async def submission_by_request(
        self,
        conversation_id: ConversationId,
        request_id: str,
        context: Context,
    ) -> SubmissionRecord | None:
        """Find a submission by its conversation-scoped host deduplication key."""

    async def find_document(
        self,
        address: DocumentAddress,
        at: DocumentPoint,
        context: Context,
    ) -> DocumentRecord | None:
        """Resolve the incarnation occupying one exact logical address at the selected point."""

    async def document(
        self,
        id: DocumentId,
        at: DocumentPoint,
        context: Context,
    ) -> StoredDocument | None:
        """Materialize one specific incarnation by ID at the selected point."""

    async def scan_documents(
        self,
        query: DocumentQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[DocumentRecord, Cursor]:
        """Scan incarnations alive in one exact scope at the selected point."""

    async def close(self, context: Context) -> None:
        """Release backend resources; all later operations must reject."""


# ── Public Session and Tx surfaces ─────────────────────────────────────────


class Tx(Protocol):
    """Transaction surface available inside one Session commit callback."""

    async def conversation(self, id: ConversationId) -> ConversationRecord | None: ...

    async def entry(self, first: Any, second: Any = None) -> EntryRecord | None: ...

    async def task(self, id: TaskId) -> TaskRecord | None: ...

    async def scan_conversations(
        self,
        query: ConversationQuery,
        limit: int,
        cursor: Cursor | None = None,
    ) -> Page[ConversationRecord, Cursor]: ...

    async def scan_entries(
        self,
        query: EntryQuery,
        limit: int,
        cursor: Cursor | None = None,
    ) -> Page[EntryRecord, Cursor]: ...

    async def latest_head_marker(self, conversation_id: ConversationId) -> EntryWithHead | None: ...

    async def scan_tasks(
        self,
        query: TaskQuery,
        limit: int,
        cursor: Cursor | None = None,
    ) -> Page[TaskRecord, Cursor]: ...

    async def submission_by_request(
        self, conversation_id: ConversationId, request_id: str
    ) -> SubmissionRecord | None: ...

    async def create_conversation(self, options: dict[str, Any]) -> ConversationRecord: ...

    async def fork_conversation(
        self,
        parent_conversation_id: ConversationId,
        at: EntryId,
        options: dict[str, Any],
    ) -> ConversationRecord: ...

    async def append_entry(self, first: Any, second: Any, third: Any = None) -> EntryRecord: ...

    async def create_task(self, task: Any, input: JsonValue, options: TaskOptions) -> TaskId: ...

    async def create_submission(self, create: SubmissionCreate) -> SubmissionRecord: ...

    def settle_submission(self, id: SubmissionId, settlement: SubmissionSettlement) -> None: ...

    def place_submission(self, id: SubmissionId, entry: EntryId) -> None: ...

    def set_task(self, value: TaskRecord) -> None: ...

    async def doc(self, token: Any, *args: Any) -> Any: ...

    async def retire_doc(self, token: Any, *args: Any) -> None: ...


class Session(Protocol):
    """Owner of one mutation line, its records, and its tracked documents."""

    async def commit(self, change: Any, context: Context) -> Any: ...

    async def close(self, context: Context) -> None: ...

    @property
    def closed(self) -> Any: ...

    def subscribe_commits(self, listener: Any) -> Any: ...

    def subscribe_close(self, listener: Any) -> Any: ...

    async def snapshot(self, token: Any, *args: Any) -> JsonObject | None: ...

    async def document_state(self, token: Any, *args: Any) -> Any: ...

    async def watch_doc(self, token: Any, *args: Any) -> Any: ...

    async def snapshot_as_of(self, token: Any, *args: Any) -> JsonObject | None: ...

    # Port addition: the Session-level read that `Conversation.context()` exposes. Upstream keeps
    # it in the Harness, which does not exist yet.
    async def context(
        self,
        conversation_id: ConversationId,
        context: Context,
        at: EntryId | None = None,
    ) -> "ContextView": ...


# ── Entry construction ─────────────────────────────────────────────────────

_ENTRY_DRAFT_FIELDS = frozenset({"kind", "model", "data", "head", "edits"})
_EDIT_FIELDS = frozenset({"target", "action", "messages"})


def _require_mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{label} must be a mapping; got {type(value).__name__}")
    for key in value:
        if not isinstance(key, str):
            raise TypeError(f"{label} must have string keys; got {key!r}")
    return cast(Mapping[str, Any], value)


def _copy_messages(value: object, label: str) -> list[MessageRecord]:
    if not isinstance(value, list):
        raise TypeError(f"{label} must be a list of messages")
    messages: list[MessageRecord] = []
    for index, item in enumerate(value):
        message = _require_mapping(item, f"{label}[{index}]")
        role = message.get("role")
        if not isinstance(role, str) or not role:
            raise TypeError(f"{label}[{index}] must have a non-empty string role")
        messages.append(cast(MessageRecord, copy_json(message)))
    return messages


def _copy_context_edits(value: object, label: str) -> list[ContextEdit]:
    if not isinstance(value, list):
        raise TypeError(f"{label} must be a list of context edits")
    edits: list[ContextEdit] = []
    for index, item in enumerate(value):
        edit = _require_mapping(item, f"{label}[{index}]")
        unknown = set(edit) - _EDIT_FIELDS
        if unknown:
            raise TypeError(f"{label}[{index}] has unknown field {sorted(unknown)[0]!r}")
        if "target" not in edit:
            raise TypeError(f"{label}[{index}] is missing target")
        if "action" not in edit:
            raise TypeError(f"{label}[{index}] is missing action")
        target = require_positive_int(edit["target"], f"{label}[{index}] target")
        action = edit["action"]
        if action == "omit":
            if "messages" in edit:
                raise TypeError(f"{label}[{index}] omit cannot carry messages")
            edits.append(cast(ContextEdit, freeze_json({"target": target, "action": "omit"})))
        elif action == "replace":
            if "messages" not in edit:
                raise TypeError(f"{label}[{index}] replace requires messages")
            messages = _copy_messages(edit["messages"], f"{label}[{index}].messages")
            edits.append(
                cast(
                    ContextEdit,
                    freeze_json({"target": target, "action": "replace", "messages": messages}),
                )
            )
        else:
            raise TypeError(f"{label}[{index}].action must be 'omit' or 'replace'")
    return edits


def _copy_entry_head(value: object, label: str) -> EntryId | Literal["self"]:
    if value == "self":
        return "self"
    return cast(EntryId, require_positive_int(value, label))


def entry_draft(value: Mapping[str, Any]) -> EntryDraft:
    """Validate and freeze an entry draft.

    Unknown fields, a missing or empty kind, malformed edits, non-JSON data, and non-message model
    entries are rejected with ``TypeError``. The returned value is a deep-frozen JSON object.
    """

    draft = _require_mapping(value, "Entry draft")
    unknown = set(draft) - _ENTRY_DRAFT_FIELDS
    if unknown:
        raise TypeError(f"Entry draft has unknown field {sorted(unknown)[0]!r}")
    kind = draft.get("kind")
    if not isinstance(kind, str) or not kind:
        raise TypeError("Entry kind must be a non-empty string")
    copied: dict[str, JsonValue] = {"kind": kind}
    if "model" in draft:
        copied["model"] = cast(JsonValue, _copy_messages(draft["model"], "Entry draft model"))
    if "data" in draft:
        copied["data"] = copy_json(draft["data"])
    if "head" in draft:
        copied["head"] = cast(JsonValue, _copy_entry_head(draft["head"], "Entry draft head"))
    if "edits" in draft:
        copied["edits"] = cast(JsonValue, _copy_context_edits(draft["edits"], "Entry draft edits"))
    return cast(EntryDraft, freeze_json(copied))


def entry_record(
    entry_id: EntryId,
    conversation_id: ConversationId,
    draft: Mapping[str, Any],
    *,
    by_task_id: TaskId | None = None,
) -> EntryRecord:
    """Build an immutable entry record from a validated draft.

    ``head: "self"`` resolves to the newly assigned entry ID. The record copies by value, so
    neither the draft nor its model messages can change it afterwards.
    """

    entry_id = cast(EntryId, require_positive_int(entry_id, "entry ID"))
    conversation_id = cast(ConversationId, require_positive_int(conversation_id, "conversation ID"))
    validated = entry_draft(draft)
    record: dict[str, JsonValue] = {
        "id": entry_id,
        "conversationId": conversation_id,
        "kind": validated["kind"],
    }
    for field in ("model", "data", "edits"):
        if field in validated:
            record[field] = copy_json(validated[field])
    if "head" in validated:
        head = validated["head"]
        record["head"] = entry_id if head == "self" else head
    if by_task_id is not None:
        record["byTaskId"] = require_positive_int(by_task_id, "task ID")
    return cast(EntryRecord, freeze_json(record))
