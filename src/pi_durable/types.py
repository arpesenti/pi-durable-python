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
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    Never,
    NotRequired,
    Protocol,
    Required,
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
    from .harness.types import ContextView, TaskRuntime

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
    "DocumentReader",
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
    "PhaseHandler",
    "RunningTask",
    "ScanOrder",
    "Seq",
    "Session",
    "SessionEnd",
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
    "TaskStateSleeping",
    "Tx",
    "TypedEntry",
    "TypedEntryDraft",
    "WatchCall",
    "WatchEnd",
    "WatchEnded",
    "WatchFailedCall",
    "WatchHandle",
    "WatchValueCall",
    "entry_draft",
    "entry_record",
    "watch_value_call",
]

MessageRecord: TypeAlias = dict[str, JsonValue]
"""One model-facing message. The pi-ai adapter supplies its concrete shape."""
Op: TypeAlias = list[Any]
"""One Chord delta operation, represented as its JSON array."""
JsonValueT_co = TypeVar("JsonValueT_co", covariant=True)


# ── Discriminant spellings (the 1.5.0 expand, #33) ───────────────────────────
#
# Upstream 1.5.0 moves six tagged unions onto the tag key ``kind``. This is the expand half of
# expand–migrate–contract: every reader of these unions takes either spelling, every writer emits
# only ``kind``, and #37 deletes the retired spelling again. Each pair lives beside its union, and
# ``to_*`` is the reader that takes either spelling and returns the one the port emits. #35 migrates
# the two pairs that move more than the key: it words ``WatchEnd`` with the ends the shipped build
# carries, and renames the environment's file-change union to the name upstream gives it.
#
#   union                 retired spelling                              current spelling
#   ``WatchEnd``          ``reason``, worded ``cancelled``/             ``kind`` — spec-1.5.0.md:5397
#                       ``session_closed``/``session_failed``/``retired``
#   ``SessionEnd``        ``reason``                                    ``kind`` — spec-1.5.0.md:1378
#   ``ReportInfo``        ``type``                                      ``kind`` — spec-1.5.0.md:680
#   ``ReportFrom``        ``type``                                      ``kind`` — upstream src/types.ts:207
#   ``DocumentCommitChange``  no tag: a retirement was ``value: null``  ``kind: "change" | "retire"``
#                                                                         beside the ``type`` that stays
#                                                                         — spec-1.5.0.md:1391
#   ``FileChange``        no tag, under the name ``WatchChange``:       ``kind: "paths" | "overflow"
#   (``env``)            which keys are present                              | "failed"``, and the name
#                                                                            ``FileChange`` — upstream
#                                                                            src/env/index.ts:127
#
# 1.5.0 leaves the tag key of these alone, read at the lines named, so nothing here accepts a
# second spelling for them and no reader converts their keys:
#
#   ``StorageWrite.type``, and ``TableCommitChange`` as its subset  — spec-1.5.0.md:5965
#   ``SubmissionRecord.type`` and ``.status``, ``SubmissionCreate``, ``SubmissionSettlement.status``
#                                                                   — spec-1.5.0.md:195 and 201
#   ``TaskState.status`` and ``TaskOutcome.status``                 — spec-1.5.0.md:2090 and 2081
#   ``DocumentContent.kind``                                        — spec-1.5.0.md:5953
#   ``DocumentScope.kind`` (the ``scope`` of ``DocumentRecord``)    — spec-1.5.0.md:1335
#
# ``harness/types.py`` carries the two report pairs and ``env/__init__.py`` the file-change pair,
# each with the same note.


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


class ContinuePlacedRecord(SubmissionRecordBase):
    """A run started without a user message: owned by that run, and never queued."""

    type: Literal["continue"]
    status: Literal["placed"]
    entry: NotRequired[Never]
    answer: NotRequired[Never]
    reason: NotRequired[Never]
    detail: NotRequired[Never]


class ContinueDoneRecord(SubmissionRecordBase):
    """The run a continue started answered."""

    type: Literal["continue"]
    status: Literal["done"]
    entry: NotRequired[Never]
    answer: EntryId
    reason: NotRequired[Never]
    detail: NotRequired[Never]


class ContinueUnansweredRecord(SubmissionRecordBase):
    """The run a continue started ended without an answer."""

    type: Literal["continue"]
    status: Literal["unanswered"]
    entry: NotRequired[Never]
    answer: NotRequired[Never]
    reason: str
    detail: NotRequired[JsonValue]


SubmissionRecord: TypeAlias = (
    InputQueuedRecord
    | InputPlacedRecord
    | InputDoneRecord
    | InputUnansweredRecord
    | WriteQueuedRecord
    | WriteDoneRecord
    | WriteUnansweredRecord
    | ContinuePlacedRecord
    | ContinueDoneRecord
    | ContinueUnansweredRecord
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


class ContinuePlacedCreate(SubmissionCreateBase):
    type: Literal["continue"]
    status: Literal["placed"]
    entry: NotRequired[Never]
    answer: NotRequired[Never]
    reason: NotRequired[Never]
    detail: NotRequired[Never]


class ContinueDoneCreate(SubmissionCreateBase):
    type: Literal["continue"]
    status: Literal["done"]
    entry: NotRequired[Never]
    answer: EntryId
    reason: NotRequired[Never]
    detail: NotRequired[Never]


class ContinueUnansweredCreate(SubmissionCreateBase):
    type: Literal["continue"]
    status: Literal["unanswered"]
    entry: NotRequired[Never]
    answer: NotRequired[Never]
    reason: str
    detail: NotRequired[JsonValue]


SubmissionCreate: TypeAlias = (
    InputQueuedCreate
    | InputPlacedCreate
    | InputDoneCreate
    | InputUnansweredCreate
    | WriteQueuedCreate
    | WriteDoneCreate
    | WriteUnansweredCreate
    | ContinuePlacedCreate
    | ContinueDoneCreate
    | ContinueUnansweredCreate
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


class TaskStateSleeping(TypedDict, Generic[S]):
    """Parked without an invocation until the Harness clock reaches ``until``."""

    status: Literal["sleeping"]
    checkpoint: S
    until: int
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
    | TaskStateSleeping[Any]
    | TaskStateCompleting[Any]
    | TaskStateTerminal[Any]
)
NextTaskState: TypeAlias = (
    TaskStateRunning[Any]
    | TaskStateWaiting[Any]
    | TaskStateSleeping[Any]
    | TaskStateTerminal[Any]
)


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
    restarts: NotRequired[int]
    """Crash cut-offs in a row without progress since; a close does not count (spec §5.4)."""


class LiveTaskRecord(TaskRecordBase):
    state: (
        TaskStatePending[Any]
        | TaskStateRunning[Any]
        | TaskStateWaiting[Any]
        | TaskStateSleeping[Any]
    )
    memos: NotRequired[dict[str, JsonValue]]


class SettledTaskRecord(TaskRecordBase):
    state: TaskStateCompleting[Any] | TaskStateTerminal[Any]
    memos: NotRequired[Never]


TaskRecord: TypeAlias = LiveTaskRecord | SettledTaskRecord


class RunningTaskRecord(TaskRecordBase):
    state: TaskStateRunning[Any]
    memos: NotRequired[dict[str, JsonValue]]


RunningTask: TypeAlias = RunningTaskRecord


PhaseHandler: TypeAlias = Callable[[RunningTask, "TaskRuntime", Context], Awaitable[None]]
"""Runs one checkpoint phase; it must commit durable progress through ``TaskRuntime.commit()``."""


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
    """One creation or update: ``type: "document"`` with ``kind: "change"`` (spec §2)."""

    type: Literal["document"]
    kind: Literal["change"]
    record: DocumentRecord
    conversationId: ConversationId | None
    version: int
    value: JsonObject
    ops: list[Op]


class DocumentRetiredChange(TypedDict):
    """The commit that retired the incarnation: ``type: "document"`` with ``kind: "retire"``.

    A retirement carries no value, version, or operations — the tag says it (spec §2).
    """

    type: Literal["document"]
    kind: Literal["retire"]
    record: DocumentRecord
    conversationId: ConversationId | None


class DocumentCopiedChange(TypedDict):
    type: Literal["document.copy"]
    record: DocumentRecord
    conversationId: ConversationId
    source: DocumentCopySource


DocumentCommitChange: TypeAlias = DocumentCommittedChange | DocumentRetiredChange | DocumentCopiedChange


class DocumentCommitChangeLegacy(TypedDict):
    """The pre-1.5.0 spelling of a document change: no ``kind``, and a retirement was told apart
    by ``value: None`` and ``version: None``; #37 deletes it."""

    type: Literal["document"]
    record: DocumentRecord
    conversationId: ConversationId | None
    version: int | None
    value: JsonObject | None
    ops: list[Op]


DocumentCommitChangeInput: TypeAlias = DocumentCommitChange | DocumentCommitChangeLegacy
"""Either spelling of a committed document change a consumer may hand the port."""


def to_document_commit_change(change: DocumentCommitChangeInput) -> DocumentCommitChange:
    """The ``kind`` spelling of one committed document change (#33, #37).

    The ``document.copy`` variant has no second tag, so it stands as written; a ``document`` change
    without ``kind`` is a retirement exactly as 1.4.0 wrote one — its ``value`` is ``None``.
    """

    if "kind" in change:
        return cast(DocumentCommitChange, change)
    if change["type"] == "document.copy":
        return change
    retired = cast(DocumentCommitChangeLegacy, change)
    if retired["value"] is None:
        return DocumentRetiredChange(
            type="document",
            kind="retire",
            record=retired["record"],
            conversationId=retired["conversationId"],
        )
    return DocumentCommittedChange(
        type="document",
        kind="change",
        record=retired["record"],
        conversationId=retired["conversationId"],
        version=cast(int, retired["version"]),
        value=retired["value"],
        ops=retired["ops"],
    )


CommitChange: TypeAlias = TableCommitChange | DocumentCommitChange


class CommitPublication(TypedDict):
    seq: Seq
    changes: list[CommitChange]


class WatchValueCall(TypedDict, Generic[JsonValueT_co]):
    """One listener call: the newest value, the operations that reach it, and a retirement.

    ``ops`` turns the previously delivered value into ``value``: one commit's exact batch while the
    listener keeps up, the folded batches of every commit since the previous call otherwise, or one
    root replacement past :data:`~pi_durable.delta.MAX_PENDING_OPS` of them (spec §9.2). ``retired``
    says the watched address went absent since the previous call, and is the only thing a retirement
    carries: its value is the last one the watch was given and its operations those still pending.
    """

    kind: Literal["value"]
    value: JsonValueT_co
    ops: list[Op]
    retired: bool


class WatchFailedCall(TypedDict):
    """A failure the source may recover from, after which the watch stays open.

    Durable's failures are final — a Session that fails ends its watches ``failed`` — so the port
    never sends one (spec §9.1). It is part of the listener's shape because the listener is written
    once, against Chord's ``WatchCall``.
    """

    kind: Literal["failed"]
    error: object


WatchCall: TypeAlias = WatchValueCall[JsonValueT_co] | WatchFailedCall
"""The one shape every watch listener takes: document, conversation view, and task graph alike."""

WatchValueT = TypeVar("WatchValueT")


def watch_value_call(
    value: WatchValueT,
    ops: list[Op] | None = None,
    retired: bool = False,
) -> WatchValueCall[Any]:
    """Build one ``value`` call: the only call Durable ever delivers (spec §9.1)."""

    return {"kind": "value", "value": value, "ops": [] if ops is None else ops, "retired": retired}


class WatchEnded(TypedDict):
    """A watch that ended without an error.

    ``stopped`` is ``stop()``'s own terminal result, ``aborted`` is the acquisition context's abort
    signal, and ``ended`` is the Session ending beneath the watch (:class:`SessionEnd`).
    """

    kind: Literal["stopped", "aborted", "ended"]


class WatchListenerErrorEnd(TypedDict):
    kind: Literal["listener_error"]
    error: BaseException


class WatchFailedEnd(TypedDict):
    kind: Literal["failed"]
    error: object


WatchEnd: TypeAlias = WatchEnded | WatchListenerErrorEnd | WatchFailedEnd
"""Terminal result of one document watch, tagged by ``kind`` (spec-1.5.0.md:5397-5401).

The words are the ones the shipped build carries, not the ones the 1.5.0 changelog claims:
``chord/src/watch.ts:16-19`` declares ``stopped``, ``aborted``, ``ended``, ``listener_error`` and
``failed``, and ``chord/src/watch.ts:46-55`` makes ``stop()`` the idempotent entry point that yields
``stopped`` while ``aborted`` comes from the acquisition context's abort signal. There is no
``cancelled`` end, and the 1.4.0 ``session_closed``, ``session_failed`` and ``retired`` ends are
gone: a watch whose document retires stays open until it is stopped or the Session ends.
"""


class WatchEndLegacy(TypedDict, total=False):
    """The pre-1.5.0 spelling of a watch end, tagged by ``reason`` and worded with the ends 1.5.0
    renamed or removed; #37 deletes it.

    ``error`` is the payload of ``listener_error`` and ``session_failed`` only; an end with another
    tag settles without it, as :func:`to_watch_end` shows.
    """

    reason: Required[
        Literal["stopped", "cancelled", "session_closed", "retired", "listener_error", "session_failed"]
    ]
    error: object


WatchEndInput: TypeAlias = WatchEnd | WatchEndLegacy
"""Either spelling of a watch end: a caller may hand the port the retired ``reason`` one."""

#: What each 1.4.0 end word reads as now: the changelog's ``cancelled`` is the shipped build's
#: ``aborted``, and the Session's own end is the ``ended``/``failed`` pair (:class:`SessionEnd`).
#: The 1.4.0 ``retired`` end is absent on purpose: there is no end to read it as.
_WATCH_END_WORDS: dict[str, str] = {
    "stopped": "stopped",
    "cancelled": "aborted",
    "session_closed": "ended",
    "listener_error": "listener_error",
    "session_failed": "failed",
}


def to_watch_end(end: WatchEndInput) -> WatchEnd:
    """The ``kind`` spelling of a watch end, whichever spelling it arrived in (#33, #35, #37).

    The retired words arrive with the retired tag key, so both move together; the 1.4.0 ``retired``
    end has no successor to read it as, because a retirement is a change now and not an end.
    """

    if "reason" not in end:
        return end
    retired = cast(WatchEndLegacy, end)
    tag = _WATCH_END_WORDS.get(retired["reason"])
    if tag is None:
        raise ValueError("A retired document has no watch end to read: a retirement is a change")
    if tag in ("listener_error", "failed"):
        return cast(WatchEnd, {"kind": tag, "error": retired.get("error")})
    return cast(WatchEnd, {"kind": tag})


class WatchHandle(Protocol, Generic[JsonValueT_co]):
    """Serialized coalesced observation of an immutable value with bounded pending delivery."""

    @property
    def value(self) -> JsonValueT_co:
        """Acquisition revision before start; latest delivered immutable revision afterward."""
        ...

    @property
    def closed(self) -> asyncio.Future[WatchEnd]:
        """Settle when the watch terminates; an already-running callback remains caller-owned."""
        ...

    def start(self, listener: Callable[[WatchCall[Any], Context], Any]) -> None:
        """Install the sole serialized asynchronous listener. Never invokes it inline."""
        ...

    def stop(self) -> asyncio.Future[WatchEnd]:
        """Idempotently stop future callbacks and return this watch's terminal result.

        The sole cancel entry point the shipped build has (``chord/src/watch.ts:46-55``): it yields
        ``{"kind": "stopped"}``, never the ``aborted`` end the acquisition context's abort emits.
        """

        ...


DocumentWatch: TypeAlias = WatchHandle[JsonObject]
"""Serialized coalesced observation of one document address."""


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
    status: NotRequired[
        Literal["pending", "running", "waiting", "sleeping", "completing", "terminal"]
    ]
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


class CommitResult(TypedDict):
    """What one :meth:`Storage.commit_batches` call stored: the durable batch count, and the
    refusal when a batch was refused.

    ``refused`` is the error Durable's own checks raised for the first batch they refused; the
    batches before it are durable and it, and every batch after it, is not. It is a resolved
    outcome, unlike a database or driver error, which rejects the call.
    """

    stored: int
    refused: NotRequired[BaseException]


CommitReceipt: TypeAlias = Callable[[int, Seq], None]
"""One durability receipt: ``receive(index, seq)`` per stored batch, in order, before the call settles."""

CommitPublicationScope: TypeAlias = Callable[[Callable[[], None], Context], None]
"""A storage's own scope for one commit's publication: it runs ``publish``, exactly once, inside it."""


@runtime_checkable
class PublishingStorage(Protocol):
    """The optional publication scope a :class:`Storage` may carry, as one companion protocol.

    Upstream declares ``publishing`` as an optional member of ``Storage``; a Python protocol has no
    optional member, so it lives here instead and the Session asks for it with ``isinstance`` rather
    than requiring it. A storage that owns a batch -- upstream's attached-host
    store, which ADR 0003 excludes -- takes each commit's publication inside that batch, so what the
    publication marks lands together with what the commit stored. It runs ``publish`` exactly once,
    now or soon after; the Session publishes the next commit only once it ran, and reports and
    publishes directly when the scope itself throws. Every bundled backend omits it, which is what
    the Session publishes directly against.
    """

    def publishing(self, publish: Callable[[], None], context: Context) -> None:
        """Run ``publish`` -- one stored commit's publication -- inside this storage's own scope."""


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
    because its caller's context was aborted. Any error from :meth:`commit_batches` or
    :meth:`mint_id` is fatal.

    ``commit_batches`` is the only commit shape: a storage implements one call and no single-write
    companion. Code that seeds or writes a storage directly, such as a test, goes through
    :func:`pi_durable.testing.commit_writes`, which passes one batch to this call.

    The optional publication scope is not a member here: see :class:`PublishingStorage`.
    """

    async def commit_batches(
        self,
        batches: Sequence[Sequence[StorageWrite]],
        receive: CommitReceipt,
        context: Context,
    ) -> CommitResult:
        """Persist ``batches`` in order, each as one atomic commit with its own sequence.

        Each batch is checked against the state the batches before it left, in as few round trips
        as the backend can make. Every batch has at least one write. The call stops at the first
        batch Durable's own rules refuse -- global IDs, document actions, copy resolution: the
        batches before it are durable, it and the batches after it are not -- and resolves with
        their number in ``stored`` and the refusal in ``refused``. Any other failure (a driver or
        I/O error, a failed ``COMMIT``) rejects the whole call and stores nothing the backend could
        roll back, and the receipts already made stand.

        ``receive(index, seq)`` runs once per stored batch, in order, once all of them are durable
        and before the next call starts; it is a durability receipt, not publication, and the
        Session publishes later.
        """

    async def mint_id(self) -> int:
        """Return a fresh ID above every ID minted since this storage was opened and every ID stored.

        The contract holds also across reopen: scans order by ID as creation order, and the Harness
        relies on it. The value is unbranded; a caller applies
        :func:`~pi_durable.ids.id_from_number` at its own trusted boundary.
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

    def close(self, context: Context) -> Awaitable[None]:
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


class DocumentReader(Protocol):
    """Read side of committed documents: ``Session.snapshot`` and ``snapshotAsOf``."""

    async def snapshot(self, token: Any, *args: Any) -> JsonObject | None: ...

    async def snapshot_as_of(self, token: Any, *args: Any) -> JsonObject | None: ...


class SessionEndClosed(TypedDict):
    """The Session ended with ``close()``."""

    kind: Literal["closed"]


class SessionEndFailed(TypedDict):
    """The Session failed and closed itself; ``error`` is the first Storage error."""

    kind: Literal["failed"]
    error: Any


SessionEnd: TypeAlias = SessionEndClosed | SessionEndFailed
"""Terminal result of one Session, tagged by ``kind`` (spec-1.5.0.md:1378, upstream
``src/types.ts:1172``). A watch of a document, view, or graph mirrors it: ``ended`` for a Session
that closed and ``failed`` for one that failed (spec-1.5.0.md:5397)."""


class SessionEndLegacy(TypedDict, total=False):
    """The pre-1.5.0 spelling of a session end, tagged by ``reason``; #37 deletes it."""

    reason: Required[Literal["closed", "failed"]]
    error: Any


SessionEndInput: TypeAlias = SessionEnd | SessionEndLegacy
"""Either spelling of a session end: a caller may hand the port the retired ``reason`` one."""


def to_session_end(end: SessionEndInput) -> SessionEnd:
    """The ``kind`` spelling of a session end, whichever spelling it arrived in (#33, #37)."""

    if "reason" not in end:
        return end
    retired = cast(SessionEndLegacy, end)
    if retired["reason"] == "failed":
        return cast(SessionEnd, {"kind": "failed", "error": retired.get("error")})
    return cast(SessionEnd, {"kind": "closed"})


class Session(Protocol):
    """Owner of one mutation line, its records, and its tracked documents."""

    async def commit(self, change: Any, context: Context) -> Any: ...

    async def close(self, context: Context) -> None: ...

    @property
    def closed(self) -> asyncio.Future[SessionEnd]: ...

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
