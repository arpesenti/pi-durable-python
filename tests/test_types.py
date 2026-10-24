"""Runtime and static translation of the reference ``types`` suite.

The reference checks most of these contracts at compile time with ``satisfies`` and
``@ts-expect-error``. Python has no compile-time discriminated unions at runtime, so the positive
shapes are both ordinary JSON values and mypy-checked ``TypedDict`` literals, while the negative
shapes live under ``TYPE_CHECKING`` and must keep producing a mypy error. CI runs mypy over the
tests, and ``warn_unused_ignores`` turns a missing error into a failure.

The reference suite's task-definition and harness cases belong to later tickets (task runtime and
Harness); this translation covers the record, identity, and storage-shape contracts of this value
layer.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from pi_durable import ToolResultEntry
from pi_durable.harness.types import HooksOf
from pi_durable.ids import (
    ConversationId,
    DocumentId,
    EntryId,
    Seq,
    SubmissionId,
    TaskId,
    id_from_number,
)
from pi_durable.types import (
    CheckpointInfo,
    CommitChange,
    CommitPublication,
    ContextEdit,
    ConversationOwnership,
    ConversationQuery,
    ConversationRecord,
    Cursor,
    DocumentAddress,
    DocumentCommitChange,
    DocumentContent,
    DocumentCopySource,
    DocumentCreate,
    DocumentPoint,
    DocumentQuery,
    DocumentRecord,
    DocumentSemantics,
    EntryDraft,
    EntryQuery,
    EntryRecord,
    EntryWithHead,
    Id,
    JoinPolicy,
    JsonObject,
    JsonValue,
    LatestConversationSemantics,
    NextTaskState,
    Page,
    PhaseHandler,
    RewindableConversationSemantics,
    RunningTask,
    ScanOrder,
    SessionDocumentCreate,
    SessionEnd,
    SettledTaskRecord,
    StorageWrite,
    StoredDocument,
    SubmissionCreate,
    SubmissionCreateBase,
    SubmissionQuery,
    SubmissionRecord,
    SubmissionSettlement,
    TableCommitChange,
    TaskOptions,
    TaskOutcome,
    TaskOutcomeError,
    TaskOwnership,
    TaskQuery,
    TaskRecord,
    TaskState,
    TaskStateTerminal,
    TypedEntry,
    TypedEntryDraft,
)

conversation_id = ConversationId(1)
entry_id = EntryId(2)
answer_id = EntryId(3)
task_id = TaskId(4)
submission_id = SubmissionId(5)
document_id = DocumentId(6)
sequence = Seq(1)

omit: ContextEdit = {"target": entry_id, "action": "omit"}
replace: ContextEdit = {"target": entry_id, "action": "replace", "messages": []}
pending: TaskState = {"status": "pending", "checkpoint": {"phase": "ready"}}
terminal: TaskState = {
    "status": "terminal",
    "outcome": {"status": "completed", "result": {"value": 1}},
}
completed_input: SubmissionRecord = {
    "id": submission_id,
    "conversationId": conversation_id,
    "type": "input",
    "status": "done",
    "entry": entry_id,
    "answer": answer_id,
}
completed_write: SubmissionRecord = {
    "id": submission_id,
    "conversationId": conversation_id,
    "type": "write",
    "status": "done",
    "entry": entry_id,
}
queued_write_create: SubmissionCreate = {
    "conversationId": conversation_id,
    "type": "write",
    "status": "queued",
}
base_content: DocumentContent = {"kind": "base", "version": 1, "value": {"count": 1}}
delta_content: DocumentContent = {"kind": "delta", "version": 1, "ops": [["s", ["count"], 2]]}
conversation_document: DocumentCreate = {
    "id": document_id,
    "kind": "test",
    "scope": {"kind": "conversation", "conversationId": conversation_id},
    "history": "rewindable",
    "fork": "asOf",
}
completed_outcome: TaskOutcome = {"status": "completed", "result": {"value": 1}}
checkpoint_info: CheckpointInfo = {"deltasSinceBase": 0}
conversation_ownership: ConversationOwnership = {"kind": "ownerless"}
task_ownership: TaskOwnership = {"kind": "conversation"}
conversation_record: ConversationRecord = {
    "id": conversation_id,
    "parent": {"conversationId": ConversationId(9), "at": EntryId(10)},
    "owner": {"conversationId": ConversationId(9), "taskId": TaskId(11)},
}
conversation_query: ConversationQuery = {"ownerTaskId": task_id, "order": "ascending"}
entry_query: EntryQuery = {"conversationId": conversation_id, "order": "descending"}
task_query: TaskQuery = {"status": "pending", "background": False}
submission_query: SubmissionQuery = {"status": "queued"}
scan_order: ScanOrder = "ascending"
join_policy: JoinPolicy = "allSettled"
cursor: Cursor = {"after": 4, "order": "ascending"}
page: Page[EntryRecord, Cursor] = {"items": []}
document_address: DocumentAddress = {"kind": "test", "scope": {"kind": "session"}}
document_point: DocumentPoint = sequence
document_query: DocumentQuery = {"scope": {"kind": "session"}, "at": "current"}
document_copy_source: DocumentCopySource = {"id": document_id, "at": sequence}
stored_document: StoredDocument = {
    "record": {
        "id": document_id,
        "kind": "test",
        "createdAt": sequence,
        "scope": {"kind": "session"},
    },
    "version": 1,
    "value": {"count": 1},
    "deltasSinceBase": 0,
}
document_semantics: DocumentSemantics = {
    "scope": "conversation",
    "history": "rewindable",
    "fork": "asOf",
}
latest_semantics: LatestConversationSemantics = {
    "scope": "conversation",
    "history": "latest",
    "fork": "initial",
}
rewindable_semantics: RewindableConversationSemantics = {
    "scope": "conversation",
    "history": "rewindable",
    "fork": "current",
}
task_outcome_error: TaskOutcomeError = {"message": "failed"}
submission_settlement: SubmissionSettlement = {"status": "done", "answer": answer_id}
task_options: TaskOptions = {
    "ownership": {"kind": "task", "taskId": task_id},
    "background": True,
}
next_state: NextTaskState = {"status": "running", "checkpoint": {"phase": "ready"}}
running_task: RunningTask = {
    "id": task_id,
    "conversationId": conversation_id,
    "kind": "test",
    "version": 1,
    "input": None,
    "background": False,
    "abortRequested": False,
    "state": {"status": "running", "checkpoint": {"phase": "ready"}},
}
typed_entry: TypedEntry[JsonValue] = {
    "id": entry_id,
    "conversationId": conversation_id,
    "kind": "app.note",
    "data": {"value": 1},
}
typed_entry_draft: TypedEntryDraft[JsonValue] = {"data": {"value": 1}}
entry_with_head: EntryWithHead = {
    "id": entry_id,
    "conversationId": conversation_id,
    "kind": "app.note",
    "head": entry_id,
}
commit_change: CommitChange = {
    "type": "entry",
    "value": typed_entry,
}
document_commit_change: DocumentCommitChange = {
    "type": "document",
    "record": {
        "id": document_id,
        "kind": "test",
        "createdAt": sequence,
        "scope": {"kind": "session"},
    },
    "conversationId": None,
    "version": 1,
    "value": {"count": 1},
    "ops": [],
}
table_commit_change: TableCommitChange = {
    "type": "conversation",
    "value": {"id": conversation_id},
}
commit_publication: CommitPublication = {"seq": sequence, "changes": [commit_change]}
json_object: JsonObject = {"count": 1}
id_value: Id = id_from_number(12)
entry_draft_value: EntryDraft = {"kind": "app.note", "head": "self"}
entry_record_value: EntryRecord = typed_entry
terminal_task: TaskRecord = {
    "id": task_id,
    "conversationId": conversation_id,
    "kind": "test",
    "version": 1,
    "input": None,
    "background": False,
    "abortRequested": False,
    "state": {"status": "terminal", "outcome": {"status": "completed", "result": 1}},
}
session_document: DocumentRecord = {
    "id": document_id,
    "kind": "test",
    "createdAt": sequence,
    "scope": {"kind": "session"},
}
conversation_write: StorageWrite = {
    "type": "conversation",
    "value": {"id": conversation_id},
}
session_end_closed: SessionEnd = {"reason": "closed"}
session_end_failed: SessionEnd = {"reason": "failed", "error": RuntimeError("disk gone")}


async def phase_handler(task: RunningTask, runtime: object, context: object) -> None:
    del task, runtime, context


handler: PhaseHandler = phase_handler
hooks: HooksOf = {"beforeTool": None}


def test_positive_record_shapes_are_plain_json_values() -> None:
    values = [
        omit,
        replace,
        pending,
        terminal,
        completed_input,
        completed_write,
        queued_write_create,
        base_content,
        delta_content,
        conversation_document,
        completed_outcome,
        checkpoint_info,
        conversation_ownership,
        task_ownership,
        conversation_record,
        conversation_query,
        entry_query,
        task_query,
        submission_query,
        cursor,
        page,
        document_address,
        document_query,
        document_copy_source,
        stored_document,
        document_semantics,
        latest_semantics,
        rewindable_semantics,
        task_outcome_error,
        submission_settlement,
        task_options,
        next_state,
        running_task,
        typed_entry,
        typed_entry_draft,
        entry_with_head,
        commit_change,
        document_commit_change,
        table_commit_change,
        commit_publication,
        json_object,
        entry_draft_value,
        entry_record_value,
        terminal_task,
        session_document,
        conversation_write,
    ]
    for value in values:
        assert isinstance(value, dict)
        assert json.loads(json.dumps(value)) == value
    for scalar in [scan_order, join_policy, document_point, id_value]:
        assert json.loads(json.dumps(scalar)) == scalar


def test_typed_dict_optionality_matches_the_record_contract() -> None:
    assert EntryRecord.__required_keys__ == frozenset({"id", "conversationId", "kind"})
    assert EntryRecord.__optional_keys__ == frozenset(
        {"model", "data", "head", "edits", "byTaskId"}
    )
    assert "checkpoint" in TaskStateTerminal.__optional_keys__
    assert "id" in SubmissionCreateBase.__optional_keys__
    assert "createdAt" in SessionDocumentCreate.__optional_keys__
    assert "memos" in SettledTaskRecord.__optional_keys__


def test_discriminators_select_the_expected_fields() -> None:
    assert omit["action"] == "omit"
    assert replace["action"] == "replace"
    assert pending["status"] == "pending"
    assert terminal["status"] == "terminal"
    assert completed_input["type"] == "input"
    assert completed_input["answer"] == answer_id
    assert completed_write["type"] == "write"
    assert queued_write_create["status"] == "queued"
    assert base_content["kind"] == "base"
    assert delta_content["kind"] == "delta"
    assert conversation_document["fork"] == "asOf"
    assert completed_outcome["status"] == "completed"
    assert session_end_closed["reason"] == "closed"
    assert session_end_failed["reason"] == "failed"
    assert handler is phase_handler
    assert hooks == {"beforeTool": None}


if TYPE_CHECKING:
    # A typed entry token narrows the record's data payload.
    def _narrowed_entry_data(entry: EntryRecord) -> None:
        if ToolResultEntry.is_(entry):
            data: dict[str, JsonValue] = entry["data"]
            assert data is not None

    # An erased task result cannot be narrowed without a typed source, and IDs of different
    # record kinds are distinct even though both are ordinary numbers at runtime.
    _wrong_task: TaskId = conversation_id  # type: ignore
    _wrong_conversation: ConversationId = task_id  # type: ignore
    _wrong_document: DocumentId = entry_id  # type: ignore
    _wrong_sequence: Seq = entry_id  # type: ignore

    # Replacement edits require replacement messages; omission edits cannot carry them.
    _missing_replacement: ContextEdit = {"target": entry_id, "action": "replace"}  # type: ignore
    _omission_with_messages: ContextEdit = {  # type: ignore
        "target": entry_id,
        "action": "omit",
        "messages": [],
    }

    # Live task state cannot carry a terminal outcome; terminal state cannot retain a checkpoint.
    _pending_with_outcome: TaskState = {  # type: ignore
        "status": "pending",
        "checkpoint": {"phase": "ready"},
        "outcome": {"status": "completed", "result": 1},
    }
    _terminal_with_checkpoint: TaskState = {  # type: ignore
        "status": "terminal",
        "checkpoint": {"phase": "ready"},
        "outcome": {"status": "completed", "result": 1},
    }

    # Terminal task records cannot retain live memos.
    _terminal_with_memos: TaskRecord = {  # type: ignore
        "id": task_id,
        "conversationId": conversation_id,
        "kind": "test",
        "version": 1,
        "input": None,
        "state": {"status": "terminal", "outcome": {"status": "completed", "result": 1}},
        "background": False,
        "abortRequested": False,
        "memos": {"retained": True},
    }

    # Session documents do not declare conversation history behavior.
    _session_with_history: DocumentRecord = {  # type: ignore
        "id": document_id,
        "kind": "test",
        "createdAt": sequence,
        "scope": {"kind": "session"},
        "history": "latest",
        "fork": "current",
    }

    # Conversation document creation requires history and fork policies.
    _conversation_without_policy: DocumentCreate = {  # type: ignore
        "id": document_id,
        "kind": "test",
        "scope": {"kind": "conversation", "conversationId": conversation_id},
    }
    # Task document creation cannot declare conversation policies.
    _task_create_with_policy: DocumentCreate = {  # type: ignore
        "id": document_id,
        "kind": "test",
        "scope": {"kind": "task", "taskId": task_id},
        "history": "latest",
        "fork": "initial",
    }
    # Storage, not the create command, supplies createdAt.
    _create_with_sequence: DocumentCreate = {  # type: ignore
        "id": document_id,
        "kind": "test",
        "scope": {"kind": "session"},
        "createdAt": sequence,
    }

    # Document bases cannot carry operation batches; deltas cannot carry materialized values.
    _base_with_ops: DocumentContent = {"kind": "base", "version": 1, "value": {}, "ops": []}  # type: ignore
    _delta_with_value: DocumentContent = {  # type: ignore
        "kind": "delta",
        "version": 1,
        "ops": [],
        "value": {},
    }
    # Document creation always starts from a complete base.
    _create_with_delta: StorageWrite = {
        "type": "document.create",
        "record": {"id": document_id, "kind": "test", "scope": {"kind": "session"}},
        "content": {"kind": "delta", "version": 1, "ops": []},  # type: ignore
    }

    # Completed outcomes cannot carry errors.
    _completed_with_error: TaskOutcome = {  # type: ignore
        "status": "completed",
        "result": 1,
        "error": {"message": "impossible"},
    }

    # Queued submissions cannot reference transcript entries.
    _queued_with_entry: SubmissionRecord = {  # type: ignore
        "id": submission_id,
        "conversationId": conversation_id,
        "type": "input",
        "status": "queued",
        "entry": entry_id,
    }
    # Successful input submissions require an answer entry.
    _input_without_answer: SubmissionRecord = {  # type: ignore
        "id": submission_id,
        "conversationId": conversation_id,
        "type": "input",
        "status": "done",
        "entry": entry_id,
    }
    # Passive write submissions never carry an answer.
    _write_with_answer: SubmissionRecord = {  # type: ignore
        "id": submission_id,
        "conversationId": conversation_id,
        "type": "write",
        "status": "done",
        "entry": entry_id,
        "answer": answer_id,
    }
    # Session, not the submission create value, assigns its ID.
    _submission_create_with_id: SubmissionCreate = {  # type: ignore
        "id": submission_id,
        "conversationId": conversation_id,
        "type": "write",
        "status": "queued",
    }
