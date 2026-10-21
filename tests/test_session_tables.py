"""Translated v1.3.0 ``session-tables`` suite (kernel slice)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from session_support import (
    context,
    create_conversation,
    document_changes,
    open_test_session,
)

from pi_durable import (
    MemoryStorage,
    Session,
    SessionFailed,
    create_session,
    define_doc,
    define_doc_family,
    define_task,
)
from pi_durable.errors import ReadAfterWrite
from pi_durable.ids import ConversationId, TaskId, id_from_number
from pi_durable.session.session import SessionImpl
from pi_durable.types import TaskRecord

WorkTask = define_task(
    {
        "name": "test.work",
        "version": 1,
        "initial": lambda _input: {"phase": "start"},
        "phases": {"start": lambda *_args: None, "next": lambda *_args: None},
        "abort": lambda *_args: None,
    }
)


class Progress:
    pass


ProgressDoc = define_doc(
    {
        "kind": "test.progress",
        "version": 1,
        "scope": "task",
        "initial": lambda: {"lines": []},
    }
)
StepDoc = define_doc_family(
    {
        "kind": "test.step",
        "version": 1,
        "scope": "task",
        "initial": lambda _seed: {"lines": []},
    }
)
NotesDoc = define_doc(
    {
        "kind": "test.notes",
        "version": 1,
        "scope": "conversation",
        "history": "rewindable",
        "fork": "asOf",
        "initial": lambda: {"text": ""},
    }
)


def terminal(task: TaskRecord) -> TaskRecord:
    return {
        **task,
        "state": {"status": "terminal", "outcome": {"status": "completed", "result": {"ok": True}}},
    }


async def create_task(
    session: SessionImpl,
    conversation_id: ConversationId,
    with_document: bool = False,
) -> TaskId:
    async def create(tx: Any) -> TaskId:
        task_id = await tx.create_task(
            WorkTask,
            {"path": "a"},
            {"ownership": {"kind": "conversation"}, "conversationId": conversation_id},
        )
        if with_document:
            (await tx.doc(ProgressDoc, task_id)).lines.append("started")
        return task_id

    return await session.commit(create, context)


async def test_allows_table_reads_only_before_the_first_table_write() -> None:
    _storage, session, _publications = open_test_session()
    conversation_id = await create_conversation(session)

    async def change(tx: Any) -> None:
        assert await tx.conversation(conversation_id) == {"id": conversation_id}
        assert await tx.scan_conversations({}, 1) == {"items": [{"id": conversation_id}]}
        assert await tx.scan_tasks({"conversationId": conversation_id}, 10) == {"items": []}
        assert await tx.scan_entries({"conversationId": conversation_id}, 10) == {"items": []}
        await tx.append_entry(conversation_id, {"kind": "note"})
        with pytest.raises(ReadAfterWrite):
            await tx.conversation(conversation_id)
        with pytest.raises(
            ReadAfterWrite,
            match=r"Tx.task\(\) cannot read tables after the first table write",
        ):
            await tx.task(TaskId(1))
        with pytest.raises(ReadAfterWrite):
            await tx.entry(id_from_number(1))
        with pytest.raises(ReadAfterWrite):
            await tx.scan_conversations({}, 10)
        with pytest.raises(ReadAfterWrite):
            await tx.scan_entries({"conversationId": conversation_id}, 10)
        # Document access remains available after table writes.
        (await tx.doc(NotesDoc, conversation_id)).text = "after write"

    await session.commit(change, context)
    assert await session.snapshot(NotesDoc, conversation_id, context) == {"text": "after write"}


async def test_passes_caller_selected_limits_and_cursors_through_table_scans() -> None:
    _storage, session, _publications = open_test_session()
    ids = [
        await create_conversation(session),
        await create_conversation(session),
        await create_conversation(session),
    ]

    async def change(tx: Any) -> None:
        first = await tx.scan_conversations({}, 2)
        assert [item["id"] for item in first["items"]] == ids[:2]
        assert first["next"] is not None
        second = await tx.scan_conversations({}, 2, first["next"])
        assert [item["id"] for item in second["items"]] == ids[2:]
        assert "next" not in second

    await session.commit(change, context)


async def test_treats_synchronous_set_task_as_the_first_table_write() -> None:
    _storage, session, _publications = open_test_session()
    conversation_id = await create_conversation(session)
    task_id = await create_task(session, conversation_id)

    async def change(tx: Any) -> None:
        task = await tx.task(task_id)
        assert task is not None
        tx.set_task(task)
        with pytest.raises(ReadAfterWrite):
            await tx.task(task_id)

    await session.commit(change, context)


async def test_creates_conversations_entries_and_tasks_with_minted_ids() -> None:
    storage, session, publications = open_test_session()

    async def change(tx: Any) -> dict[str, Any]:
        conversation = await tx.create_conversation({"ownership": {"kind": "ownerless"}})
        first = await tx.append_entry(conversation["id"], {"kind": "note", "data": "one"})
        headed = await tx.append_entry(conversation["id"], {"kind": "summary", "head": "self"})
        task = await tx.create_task(
            WorkTask,
            {"path": "x"},
            {
                "ownership": {"kind": "conversation"},
                "conversationId": conversation["id"],
                "background": True,
            },
        )
        return {"conversation": conversation, "first": first, "headed": headed, "task": task}

    created = await session.commit(change, context)
    ids = [created["conversation"]["id"], created["first"]["id"], created["headed"]["id"], created["task"]]
    assert len(set(ids)) == 4
    assert created["headed"]["head"] == created["headed"]["id"]
    assert created["first"] == {
        "id": created["first"]["id"],
        "conversationId": created["conversation"]["id"],
        "kind": "note",
        "data": "one",
    }
    stored = await storage.entry(created["headed"]["id"], context)
    assert stored is not None
    assert stored["entry"] == created["headed"]
    assert await storage.task(created["task"], context) == {
        "id": created["task"],
        "conversationId": created["conversation"]["id"],
        "kind": "test.work",
        "version": 1,
        "input": {"path": "x"},
        "background": True,
        "abortRequested": False,
        "state": {"status": "pending", "checkpoint": {"phase": "start"}},
    }
    changes = publications[-1]["changes"]
    assert len(changes) == 4
    admitted = storage.admitted_commits[-1]
    assert all(any(write is change for write in admitted) for change in changes)
    assert {change["type"] for change in changes} == {"conversation", "entry", "task"}
    conversation_change = next(change for change in changes if change["type"] == "conversation")
    assert conversation_change["value"] is created["conversation"]
    entries = [change["value"] for change in changes if change["type"] == "entry"]
    assert created["first"] in entries
    assert created["headed"] in entries

    with pytest.raises(TypeError, match="requires options.conversationId"):
        await session.commit(
            lambda tx: tx.create_task(
                WorkTask, {"path": "x"}, {"ownership": {"kind": "conversation"}}
            ),
            context,
        )
    with pytest.raises(ValueError, match="Conversation 12345 does not exist"):
        await session.commit(
            lambda tx: tx.append_entry(ConversationId(12345), {"kind": "note"}), context
        )


async def test_creates_a_conversation_with_an_explicitly_staged_task_owner() -> None:
    storage, session, _publications = open_test_session()
    parent_id = await create_conversation(session)

    async def change(tx: Any) -> dict[str, Any]:
        supervisor_id = await tx.create_task(
            WorkTask,
            {"path": "background"},
            {"ownership": {"kind": "conversation"}, "conversationId": parent_id, "background": True},
        )
        child = await tx.create_conversation(
            {"ownership": {"kind": "task", "taskId": supervisor_id}}
        )
        return {"supervisor_id": supervisor_id, "child": child}

    created = await session.commit(change, context)
    assert created["child"]["owner"] == {
        "conversationId": parent_id,
        "taskId": created["supervisor_id"],
    }
    assert await storage.conversation(created["child"]["id"], context) == created["child"]

    async def move(tx: Any) -> None:
        supervisor = await tx.task(created["supervisor_id"])
        assert supervisor is not None
        tx.set_task({**supervisor, "conversationId": created["child"]["id"]})

    with pytest.raises(ValueError, match=f"Task {created['supervisor_id']} cannot change conversations"):
        await session.commit(move, context)
    stored = await storage.task(created["supervisor_id"], context)
    assert stored is not None
    assert stored["conversationId"] == parent_id


async def test_rejects_missing_terminal_and_abort_marked_conversation_owners_atomically() -> None:
    storage, session, _publications = open_test_session()
    parent_id = await create_conversation(session)
    moved_staged_task_id: TaskId | None = None

    async def move(tx: Any) -> None:
        nonlocal moved_staged_task_id
        task_id = await tx.create_task(
            WorkTask,
            {"path": "move"},
            {"ownership": {"kind": "conversation"}, "conversationId": parent_id},
        )
        moved_staged_task_id = task_id
        tx.set_task(
            {
                "id": task_id,
                "conversationId": ConversationId(998),
                "kind": WorkTask.definition.name,
                "version": WorkTask.definition.version,
                "input": {"path": "move"},
                "background": False,
                "abortRequested": False,
                "state": {"status": "pending", "checkpoint": {"phase": "start"}},
            }
        )

    with pytest.raises(ValueError, match="cannot change conversations"):
        await session.commit(move, context)
    assert moved_staged_task_id is not None
    assert await storage.task(moved_staged_task_id, context) is None

    missing_task_id = TaskId(999)
    with pytest.raises(ValueError, match="Conversation owner task 999 does not exist"):
        await session.commit(
            lambda tx: tx.create_conversation(
                {"ownership": {"kind": "task", "taskId": missing_task_id}}
            ),
            context,
        )

    rejected_child_id: ConversationId | None = None

    async def aborting(tx: Any) -> None:
        nonlocal rejected_child_id
        supervisor_id = await tx.create_task(
            WorkTask,
            {"path": "aborting"},
            {"ownership": {"kind": "conversation"}, "conversationId": parent_id},
        )
        child = await tx.create_conversation(
            {"ownership": {"kind": "task", "taskId": supervisor_id}}
        )
        rejected_child_id = child["id"]
        tx.set_task(
            {
                "id": supervisor_id,
                "conversationId": parent_id,
                "kind": WorkTask.definition.name,
                "version": WorkTask.definition.version,
                "input": {"path": "aborting"},
                "background": False,
                "abortRequested": True,
                "state": {"status": "pending", "checkpoint": {"phase": "start"}},
            }
        )

    with pytest.raises(ValueError, match="is abort-marked"):
        await session.commit(aborting, context)
    assert rejected_child_id is not None
    assert await storage.conversation(rejected_child_id, context) is None

    async def finishing(tx: Any) -> None:
        supervisor_id = await tx.create_task(
            WorkTask,
            {"path": "terminal"},
            {"ownership": {"kind": "conversation"}, "conversationId": parent_id},
        )
        await tx.create_conversation({"ownership": {"kind": "task", "taskId": supervisor_id}})
        tx.set_task(
            {
                "id": supervisor_id,
                "conversationId": parent_id,
                "kind": WorkTask.definition.name,
                "version": WorkTask.definition.version,
                "input": {"path": "terminal"},
                "background": False,
                "abortRequested": False,
                "state": {"status": "terminal", "outcome": {"status": "completed", "result": {"ok": True}}},
            }
        )

    with pytest.raises(ValueError, match="is terminal"):
        await session.commit(finishing, context)

    terminal_owner_id = await create_task(session, parent_id)

    async def settle(tx: Any) -> None:
        task = await tx.task(terminal_owner_id)
        assert task is not None
        tx.set_task(terminal(task))

    await session.commit(settle, context)
    with pytest.raises(ValueError, match="is terminal"):
        await session.commit(
            lambda tx: tx.create_conversation(
                {"ownership": {"kind": "task", "taskId": terminal_owner_id}}
            ),
            context,
        )


async def test_takes_ownership_of_table_json_and_rejects_non_strict_values() -> None:
    storage, session, _publications = open_test_session()
    conversation_id = await create_conversation(session)
    payload = {"nested": {"value": 1}}

    async def change(tx: Any) -> Any:
        created = await tx.append_entry(conversation_id, {"kind": "data", "data": payload})
        payload["nested"]["value"] = 2
        return created

    entry = await session.commit(change, context)
    stored = await storage.entry(entry["id"], context)
    assert stored is not None
    assert stored["entry"]["data"] == {"nested": {"value": 1}}

    omitted = await session.commit(
        lambda tx: tx.append_entry(conversation_id, {"kind": "omitted"}), context
    )
    assert "data" not in omitted
    stored_omitted = await storage.entry(omitted["id"], context)
    assert stored_omitted is not None
    assert "data" not in stored_omitted["entry"]

    commits = len(storage.commits)
    with pytest.raises(TypeError, match="strict JSON"):
        await session.commit(
            lambda tx: tx.append_entry(
                conversation_id, {"kind": "invalid", "data": float("nan")}
            ),
            context,
        )
    assert len(storage.commits) == commits


async def test_replaces_task_records_completely() -> None:
    storage, session, _publications = open_test_session()
    conversation_id = await create_conversation(session)
    task_id = await create_task(session, conversation_id)

    async def change(tx: Any) -> None:
        task = await tx.task(task_id)
        assert task is not None
        tx.set_task(
            {
                "id": task["id"],
                "conversationId": task["conversationId"],
                "kind": task["kind"],
                "version": task["version"],
                "input": task["input"],
                "background": task["background"],
                "abortRequested": task["abortRequested"],
                "state": {"status": "running", "checkpoint": {"phase": "next", "step": 2}},
                "memos": {"choice": "b"},
            }
        )

    await session.commit(change, context)
    stored = await storage.task(task_id, context)
    assert stored is not None
    assert stored["state"] == {"status": "running", "checkpoint": {"phase": "next", "step": 2}}
    assert stored["memos"] == {"choice": "b"}


async def test_creates_a_task_and_then_its_document_in_one_transaction() -> None:
    _storage, session, publications = open_test_session()
    conversation_id = await create_conversation(session)

    async def create(tx: Any) -> TaskId:
        task_id = await tx.create_task(
            WorkTask,
            {"path": "a"},
            {"ownership": {"kind": "conversation"}, "conversationId": conversation_id},
        )
        (await tx.doc(ProgressDoc, task_id)).lines.append("created")
        (await tx.doc(StepDoc, task_id, "one", None)).lines.append("step")
        return task_id

    task_id = await session.commit(create, context)
    assert await session.snapshot(ProgressDoc, task_id, context) == {"lines": ["created"]}
    publication = publications[-1]
    documents = document_changes(publication)
    assert any(
        change["type"] == "task" and change["value"]["id"] == task_id
        for change in publication["changes"]
    )
    assert len(documents) == 2
    for document in documents:
        assert document["conversationId"] == conversation_id

    async def update(tx: Any) -> None:
        await tx.create_conversation({"ownership": {"kind": "ownerless"}})
        (await tx.doc(ProgressDoc, task_id)).lines.append("committed task")

    await session.commit(update, context)
    assert document_changes(publications[-1])[0]["conversationId"] == conversation_id


async def test_rejects_task_documents_after_a_terminal_candidate() -> None:
    _storage, session, _publications = open_test_session()
    conversation_id = await create_conversation(session)
    task_id = await create_task(session, conversation_id, True)

    async def change(tx: Any) -> None:
        task = await tx.task(task_id)
        assert task is not None
        progress = await tx.doc(ProgressDoc, task_id)
        tx.set_task(terminal(task))
        with pytest.raises(ValueError, match=f"Task {task_id} is terminal"):
            await tx.doc(ProgressDoc, task_id)
        with pytest.raises(ValueError, match=f"Task {task_id} is terminal"):
            await tx.doc(StepDoc, task_id, "late", None)
        with pytest.raises(ValueError, match="terminal candidate"):
            tx.set_task(task)
        progress.lines.append("final")

    await session.commit(change, context)
    with pytest.raises(ValueError, match=f"Task {task_id} is terminal"):
        await session.commit(lambda tx: tx.doc(ProgressDoc, task_id), context)


async def test_retires_task_documents_at_terminal_settlement() -> None:
    storage, session, publications = open_test_session()
    conversation_id = await create_conversation(session)
    task_id = await create_task(session, conversation_id, True)

    async def add_step(tx: Any) -> None:
        (await tx.doc(StepDoc, task_id, "committed", None)).lines.append("x")

    await session.commit(add_step, context)
    published = len(publications)

    async def finish(tx: Any) -> None:
        task = await tx.task(task_id)
        assert task is not None
        (await tx.doc(StepDoc, task_id, "new", None)).lines.append("created then retired")
        tx.set_task(terminal(task))

    await session.commit(finish, context)
    writes = storage.commits[-1]
    assert len(writes) == 5
    assert len([write for write in writes if write["type"] == "task"]) == 1
    creations = [write for write in writes if write["type"] == "document.create"]
    retirements = [write for write in writes if write["type"] == "document.retire"]
    assert len(creations) == 1
    assert len(retirements) == 3
    assert creations[0]["record"]["id"] in [write["id"] for write in retirements]
    assert len(publications) == published + 1
    publication = publications[-1]
    documents = document_changes(publication)
    assert len([change for change in publication["changes"] if change["type"] == "task"]) == 1
    assert [document["value"] for document in documents] == [None, None, None]
    assert all(document["ops"] == [] for document in documents)
    assert all(document["conversationId"] == conversation_id for document in documents)
    assert await session.snapshot(ProgressDoc, task_id, context) is None
    assert await session.snapshot(StepDoc, task_id, "committed", context) is None
    alive = await storage.scan_documents(
        {"scope": {"kind": "task", "taskId": task_id}, "at": "current"}, 10, None, context
    )
    assert alive["items"] == []

    async def again(tx: Any) -> None:
        task = await tx.task(task_id)
        assert task is not None
        tx.set_task(terminal(task))

    with pytest.raises(ValueError, match=f"Task {task_id} is already terminal"):
        await session.commit(again, context)


async def test_validates_document_owners() -> None:
    _storage, session, _publications = open_test_session()
    with pytest.raises(ValueError, match="Task 4242 does not exist"):
        await session.commit(lambda tx: tx.doc(ProgressDoc, TaskId(4242)), context)
    with pytest.raises(ValueError, match="Conversation 4242 does not exist"):
        await session.commit(lambda tx: tx.doc(NotesDoc, ConversationId(4242)), context)


async def test_create_session_opens_over_memory_storage() -> None:
    opened: Session = create_session(MemoryStorage())

    async def create(tx: Any) -> Any:
        conversation = await tx.create_conversation({"ownership": {"kind": "ownerless"}})
        await tx.append_entry(conversation["id"], {"kind": "note", "data": "first"})
        return conversation

    conversation = await opened.commit(create, context)
    assert conversation["id"] == 2
    await opened.close(context)
    assert await opened.closed == {"reason": "closed"}


async def test_storage_failure_fails_the_session_and_publishes_nothing() -> None:
    storage, session, publications = open_test_session()
    conversation_id = await create_conversation(session)
    published = len(publications)
    failure = RuntimeError("disk failed")
    storage.fail_next_commit(failure)

    with pytest.raises(RuntimeError, match="disk failed"):
        await session.commit(
            lambda tx: tx.append_entry(conversation_id, {"kind": "lost"}), context
        )
    assert len(publications) == published
    assert session.failure is failure
    with pytest.raises(SessionFailed) as raised:
        await session.commit(
            lambda tx: tx.append_entry(conversation_id, {"kind": "lost"}), context
        )
    assert raised.value.cause is failure
    closed = await asyncio.wait_for(session.closed, 1)
    assert closed == {"reason": "failed", "error": failure}


async def test_a_failing_storage_close_fails_the_session_and_rejects_close() -> None:
    class FailingCloseStorage(MemoryStorage):
        async def close(self, close_context: Any) -> None:
            raise RuntimeError("close failed")

    session = create_session(FailingCloseStorage())
    with pytest.raises(RuntimeError, match="close failed"):
        await session.close(context)
    closed = await asyncio.wait_for(session.closed, 1)
    assert closed["reason"] == "failed"
    assert isinstance(closed["error"], RuntimeError)


async def test_a_failed_commit_publishes_neither_the_conversation_nor_its_entries() -> None:
    storage, session, publications = open_test_session()
    published = len(publications)
    created_id: int | None = None

    async def fail(tx: Any) -> None:
        nonlocal created_id
        created = await tx.create_conversation({"ownership": {"kind": "ownerless"}})
        created_id = created["id"]
        await tx.append_entry(created["id"], {"kind": "note"})
        raise RuntimeError("callback failed")

    with pytest.raises(RuntimeError, match="callback failed"):
        await session.commit(fail, context)
    assert len(publications) == published
    assert created_id is not None
    assert await storage.conversation(ConversationId(created_id), context) is None


async def test_ids_are_never_reused_after_a_failed_commit() -> None:
    _storage, session, _publications = open_test_session()
    conversation_id = await create_conversation(session)

    async def fail(tx: Any) -> None:
        await tx.append_entry(conversation_id, {"kind": "lost"})
        raise RuntimeError("callback failed")

    with pytest.raises(RuntimeError, match="callback failed"):
        await session.commit(fail, context)

    async def succeed(tx: Any) -> Any:
        return await tx.append_entry(conversation_id, {"kind": "kept"})

    entry = await session.commit(succeed, context)
    assert entry["id"] > conversation_id
