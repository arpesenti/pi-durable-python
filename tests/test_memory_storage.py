"""Translated v1.3.0 ``memory-storage`` suite and storage conformance cases."""

from __future__ import annotations

from typing import Any, cast

import pytest

from pi_durable import MemoryStorage
from pi_durable.context import BACKGROUND_CONTEXT as context
from pi_durable.errors import StorageRequestError
from pi_durable.ids import ROOT_CONVERSATION_ID, ConversationId, EntryId
from pi_durable.types import DocumentCreate


def entry(id: int, conversation_id: int, kind: str = "message", **extra: Any) -> dict[str, Any]:
    return {"id": EntryId(id), "conversationId": ConversationId(conversation_id), "kind": kind, **extra}


def pending_task(id: int, conversation_id: int, phase: str = "ready") -> dict[str, Any]:
    return {
        "id": id,
        "conversationId": ConversationId(conversation_id),
        "kind": "test.task",
        "version": 1,
        "input": {"value": id},
        "state": {"status": "pending", "checkpoint": {"phase": phase}},
        "background": False,
        "abortRequested": False,
    }


async def create_root(storage: MemoryStorage) -> ConversationId:
    await storage.commit(
        [{"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}}], context
    )
    return ROOT_CONVERSATION_ID


async def test_reserves_id_one_for_the_immutable_root_conversation() -> None:
    storage = MemoryStorage()
    assert await storage.mint_id() == 2
    await create_root(storage)
    assert await storage.conversation(ROOT_CONVERSATION_ID, context) == {"id": ROOT_CONVERSATION_ID}
    with pytest.raises(ValueError, match=f"ID {ROOT_CONVERSATION_ID} already belongs to conversation"):
        await storage.commit(
            [{"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}}], context
        )


async def test_commits_mixed_table_writes_atomically_and_rolls_all_back_on_failure() -> None:
    storage = MemoryStorage()
    root_id = await create_root(storage)
    entry_id = await storage.mint_id()
    task_id = await storage.mint_id()
    submission_id = await storage.mint_id()
    task = pending_task(task_id, root_id)
    submission = {
        "id": submission_id,
        "conversationId": root_id,
        "requestId": "request-1",
        "type": "input",
        "status": "placed",
        "entry": entry_id,
    }
    initial_seq = await storage.commit(
        [
            {"type": "entry", "value": entry(entry_id, root_id, "user", data={"text": "hello"})},
            {"type": "task", "value": task},
            {"type": "submission", "value": submission},
        ],
        context,
    )
    assert await storage.entry(entry_id, context) == {
        "entry": entry(entry_id, root_id, "user", data={"text": "hello"}),
        "commitSeq": initial_seq,
    }
    assert await storage.task(task_id, context) == task
    assert await storage.submission(submission_id, context) == submission

    transient_id = await storage.mint_id()
    running = {**task, "state": {"status": "running", "checkpoint": {"phase": "effect"}}}
    done = {**submission, "status": "done", "answer": transient_id}
    with pytest.raises(ValueError, match=f"ID {root_id} already belongs to conversation"):
        await storage.commit(
            [
                {"type": "task", "value": running},
                {"type": "submission", "value": done},
                {"type": "entry", "value": entry(transient_id, root_id, "assistant")},
                {"type": "conversation", "value": {"id": root_id}},
            ],
            context,
        )
    assert await storage.task(task_id, context) == task
    assert await storage.submission(submission_id, context) == submission
    assert await storage.entry(transient_id, context) is None
    after = await storage.commit(
        [{"type": "entry", "value": entry(await storage.mint_id(), root_id, "after-rollback")}],
        context,
    )
    assert after > initial_seq


async def test_detaches_retained_writes_and_every_returned_record() -> None:
    storage = MemoryStorage()
    root_id = await create_root(storage)
    entry_id = await storage.mint_id()
    task_id = await storage.mint_id()
    submission_id = await storage.mint_id()
    entry_data = {"nested": [1, 2]}
    checkpoint = {"phase": "ready", "nested": {"count": 1}}
    detail = {"codes": ["initial"]}
    stored_entry = entry(entry_id, root_id, "note", data=entry_data)
    stored_task = {**pending_task(task_id, root_id), "state": {"status": "pending", "checkpoint": checkpoint}}
    stored_submission = {
        "id": submission_id,
        "conversationId": root_id,
        "type": "input",
        "status": "unanswered",
        "reason": "failed",
        "detail": detail,
    }
    await storage.commit(
        [
            {"type": "entry", "value": stored_entry},
            {"type": "task", "value": stored_task},
            {"type": "submission", "value": stored_submission},
        ],
        context,
    )
    entry_data["nested"].append(3)
    checkpoint["nested"]["count"] = 2
    detail["codes"].append("mutated")
    read_entry = await storage.entry(entry_id, context)
    assert read_entry is not None and read_entry["entry"]["data"] == {"nested": [1, 2]}
    read_task = await storage.task(task_id, context)
    assert read_task is not None and read_task["state"] == {
        "status": "pending",
        "checkpoint": {"phase": "ready", "nested": {"count": 1}},
    }
    read_submission = await storage.submission(submission_id, context)
    assert read_submission is not None and read_submission["detail"] == {"codes": ["initial"]}

    read_entry["entry"]["data"]["nested"].append(9)
    read_task["state"]["checkpoint"]["nested"]["count"] = 9
    read_submission["detail"]["codes"].append("read mutation")
    assert (await storage.entry(entry_id, context))["entry"]["data"] == {"nested": [1, 2]}
    assert (await storage.task(task_id, context))["state"] == {
        "status": "pending",
        "checkpoint": {"phase": "ready", "nested": {"count": 1}},
    }
    assert (await storage.submission(submission_id, context))["detail"] == {"codes": ["initial"]}


async def test_does_not_expose_retained_state_through_a_prepared_commit() -> None:
    storage = MemoryStorage()
    await storage.commit([{"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}}], context)
    entry_id = EntryId(2)
    prepared = storage.prepare_commit(
        [
            {
                "type": "entry",
                "value": {
                    "id": entry_id,
                    "conversationId": ROOT_CONVERSATION_ID,
                    "kind": "test",
                    "data": {"nested": [1]},
                },
            }
        ]
    )
    exposed = prepared.writes[0]
    assert exposed["type"] == "entry"
    with pytest.raises(TypeError):
        exposed["value"]["data"]["nested"].append(2)

    assert prepared.apply() == 2
    assert prepared.apply() == 2
    stored = await storage.entry(entry_id, context)
    assert stored is not None and stored["entry"]["data"] == {"nested": [1]}


async def test_indexes_entries_committed_out_of_id_order() -> None:
    storage = MemoryStorage()
    root_id = await create_root(storage)
    await storage.commit(
        [
            {"type": "entry", "value": entry(30, root_id)},
            {"type": "entry", "value": entry(10, root_id)},
            {"type": "entry", "value": entry(20, root_id, "marker", head=EntryId(10))},
        ],
        context,
    )
    page = await storage.scan_entries({"conversationId": root_id}, 10, None, context)
    assert [item["id"] for item in page["items"]] == [30, 20, 10]
    marker = await storage.find_latest_head_marker(root_id, None, context)
    assert marker is not None and marker["id"] == 20


async def test_continues_an_entry_cursor_below_its_last_item_after_a_newer_commit() -> None:
    storage = MemoryStorage()
    root_id = await create_root(storage)
    oldest = await storage.mint_id()
    middle = await storage.mint_id()
    newest = await storage.mint_id()
    await storage.commit(
        [
            {"type": "entry", "value": entry(oldest, root_id)},
            {"type": "entry", "value": entry(middle, root_id)},
            {"type": "entry", "value": entry(newest, root_id)},
        ],
        context,
    )
    first = await storage.scan_entries({"conversationId": root_id}, 2, None, context)
    assert [item["id"] for item in first["items"]] == [newest, middle]
    appended = await storage.mint_id()
    await storage.commit([{"type": "entry", "value": entry(appended, root_id)}], context)
    second = await storage.scan_entries(
        {"conversationId": root_id}, 2, first.get("next"), context
    )
    assert [item["id"] for item in second["items"]] == [oldest]
    assert "next" not in second


async def test_paginates_conversations_by_opaque_cursor_in_ascending_id_order() -> None:
    storage = MemoryStorage()
    root_id = await create_root(storage)
    second_id = await storage.mint_id()
    third_id = await storage.mint_id()
    await storage.commit(
        [
            {"type": "conversation", "value": {"id": third_id}},
            {"type": "conversation", "value": {"id": second_id}},
        ],
        context,
    )
    first = await storage.scan_conversations({}, 2, None, context)
    assert [item["id"] for item in first["items"]] == [root_id, second_id]
    assert first.get("next") is not None
    second = await storage.scan_conversations({}, 2, first["next"], context)
    assert [item["id"] for item in second["items"]] == [third_id]
    assert "next" not in second


async def test_filters_and_pages_conversations_by_durable_owner_edges() -> None:
    storage = MemoryStorage()
    root_id = await create_root(storage)
    other_owner_id = await storage.mint_id()
    first_task_id = await storage.mint_id()
    second_task_id = await storage.mint_id()
    first_id = await storage.mint_id()
    second_id = await storage.mint_id()
    third_id = await storage.mint_id()
    await storage.commit(
        [
            {"type": "conversation", "value": {"id": other_owner_id}},
            {
                "type": "conversation",
                "value": {"id": first_id, "owner": {"conversationId": root_id, "taskId": first_task_id}},
            },
            {
                "type": "conversation",
                "value": {"id": second_id, "owner": {"conversationId": root_id, "taskId": second_task_id}},
            },
            {
                "type": "conversation",
                "value": {
                    "id": third_id,
                    "owner": {"conversationId": other_owner_id, "taskId": first_task_id},
                },
            },
        ],
        context,
    )
    first = await storage.scan_conversations({"ownerConversationId": root_id}, 1, None, context)
    assert [item["id"] for item in first["items"]] == [first_id]
    assert first.get("next") is not None
    second = await storage.scan_conversations(
        {"ownerConversationId": root_id}, 1, first["next"], context
    )
    assert [item["id"] for item in second["items"]] == [second_id]
    assert "next" not in second
    by_task = await storage.scan_conversations({"ownerTaskId": first_task_id}, 10, None, context)
    assert [item["id"] for item in by_task["items"]] == [first_id, third_id]
    both = await storage.scan_conversations(
        {"ownerConversationId": root_id, "ownerTaskId": first_task_id}, 10, None, context
    )
    assert [item["id"] for item in both["items"]] == [first_id]


async def test_scans_deep_fork_history_through_every_ancestor_cap() -> None:
    storage = MemoryStorage()
    root_id = await create_root(storage)
    root_first = await storage.mint_id()
    root_fork_point = await storage.mint_id()
    root_excluded_same_commit = await storage.mint_id()
    root_seq = await storage.commit(
        [
            {"type": "entry", "value": entry(root_first, root_id)},
            {"type": "entry", "value": entry(root_fork_point, root_id, "marker", head=root_first)},
            {"type": "entry", "value": entry(root_excluded_same_commit, root_id)},
        ],
        context,
    )
    child_id = await storage.mint_id()
    await storage.commit(
        [
            {
                "type": "conversation",
                "value": {"id": child_id, "parent": {"conversationId": root_id, "at": root_fork_point}},
            }
        ],
        context,
    )
    child_fork_point = await storage.mint_id()
    child_excluded = await storage.mint_id()
    await storage.commit(
        [
            {"type": "entry", "value": entry(child_fork_point, child_id, "note")},
            {"type": "entry", "value": entry(child_excluded, child_id)},
        ],
        context,
    )
    root_excluded_later = await storage.mint_id()
    await storage.commit([{"type": "entry", "value": entry(root_excluded_later, root_id)}], context)
    grandchild_id = await storage.mint_id()
    await storage.commit(
        [
            {
                "type": "conversation",
                "value": {
                    "id": grandchild_id,
                    "parent": {"conversationId": child_id, "at": child_fork_point},
                },
            }
        ],
        context,
    )
    grandchild_head = await storage.mint_id()
    grandchild_tail = await storage.mint_id()
    grandchild_seq = await storage.commit(
        [
            {"type": "entry", "value": entry(grandchild_head, grandchild_id, "marker", head=grandchild_head)},
            {"type": "entry", "value": entry(grandchild_tail, grandchild_id)},
        ],
        context,
    )
    child_excluded_later = await storage.mint_id()
    await storage.commit([{"type": "entry", "value": entry(child_excluded_later, child_id)}], context)

    first = await storage.scan_entries({"conversationId": grandchild_id}, 2, None, context)
    assert [item["id"] for item in first["items"]] == [grandchild_tail, grandchild_head]
    second = await storage.scan_entries(
        {"conversationId": grandchild_id}, 2, first.get("next"), context
    )
    assert [item["id"] for item in second["items"]] == [child_fork_point, root_fork_point]
    third = await storage.scan_entries(
        {"conversationId": grandchild_id}, 2, second.get("next"), context
    )
    assert [item["id"] for item in third["items"]] == [root_first]
    assert "next" not in third

    ascending = {"conversationId": grandchild_id, "order": "ascending"}
    first_up = await storage.scan_entries(ascending, 2, None, context)
    assert [item["id"] for item in first_up["items"]] == [root_first, root_fork_point]
    second_up = await storage.scan_entries(
        {"conversationId": grandchild_id}, 2, first_up.get("next"), context
    )
    assert [item["id"] for item in second_up["items"]] == [child_fork_point, grandchild_head]
    third_up = await storage.scan_entries(
        {"conversationId": grandchild_id}, 2, second_up.get("next"), context
    )
    assert [item["id"] for item in third_up["items"]] == [grandchild_tail]
    bounded = await storage.scan_entries(
        {
            "conversationId": grandchild_id,
            "order": "ascending",
            "minEntryId": root_fork_point,
            "maxEntryId": child_fork_point,
        },
        10,
        None,
        context,
    )
    assert [item["id"] for item in bounded["items"]] == [root_fork_point, child_fork_point]
    with pytest.raises(StorageRequestError, match="cursor"):
        await storage.scan_entries(
            {"conversationId": grandchild_id, "order": "descending"}, 2, first_up["next"], context
        )

    current_marker = await storage.find_latest_head_marker(grandchild_id, None, context)
    assert current_marker is not None
    assert (current_marker["id"], current_marker["head"]) == (grandchild_head, grandchild_head)
    historical = await storage.find_latest_head_marker(grandchild_id, child_fork_point, context)
    assert historical is not None
    assert (historical["id"], historical["head"]) == (root_fork_point, root_first)
    assert await storage.find_latest_head_marker(grandchild_id, root_first, context) is None

    assert await storage.entry(root_first, context) == {
        "entry": entry(root_first, root_id),
        "commitSeq": root_seq,
    }
    assert (await storage.entry(grandchild_head, context))["commitSeq"] == grandchild_seq
    visible = await storage.entry(grandchild_id, child_fork_point, context)
    assert visible is not None and visible["entry"]["conversationId"] == child_id
    assert await storage.entry(grandchild_id, root_excluded_same_commit, context) is None
    assert await storage.entry(grandchild_id, root_excluded_later, context) is None
    assert await storage.entry(grandchild_id, child_excluded, context) is None
    assert await storage.entry(grandchild_id, child_excluded_later, context) is None
    with pytest.raises(StorageRequestError, match="Unknown conversation"):
        await storage.entry(ConversationId(999_999), root_first, context)
    with pytest.raises(StorageRequestError, match="Unknown conversation"):
        await storage.scan_entries({"conversationId": ConversationId(999_999)}, 10, None, context)


async def test_scans_tables_in_either_id_order_and_continues_a_cursor_in_its_order() -> None:
    storage = MemoryStorage()
    root_id = await create_root(storage)
    conversation_ids = [root_id]
    task_ids: list[int] = []
    submission_ids: list[int] = []
    for _ in range(3):
        conversation_id = await storage.mint_id()
        task_id = await storage.mint_id()
        submission_id = await storage.mint_id()
        await storage.commit(
            [
                {"type": "conversation", "value": {"id": conversation_id}},
                {"type": "task", "value": pending_task(task_id, root_id)},
                {
                    "type": "submission",
                    "value": {
                        "id": submission_id,
                        "conversationId": root_id,
                        "type": "input",
                        "status": "queued",
                    },
                },
            ],
            context,
        )
        conversation_ids.append(conversation_id)
        task_ids.append(task_id)
        submission_ids.append(submission_id)

    async def all_ids(scan: Any, order: str | None) -> list[int]:
        found: list[int] = []
        page = await scan(order, None)
        found.extend(item["id"] for item in page["items"])
        while page.get("next") is not None:
            page = await scan(None, page["next"])
            found.extend(item["id"] for item in page["items"])
        return found

    scans = [
        (
            lambda order, cursor: storage.scan_conversations(
                {"order": order}, 2, cursor, context
            ),
            conversation_ids,
        ),
        (
            lambda order, cursor: storage.scan_tasks({"order": order}, 2, cursor, context),
            task_ids,
        ),
        (
            lambda order, cursor: storage.scan_submissions(
                {"order": order}, 2, cursor, context
            ),
            submission_ids,
        ),
    ]
    for scan, ids in scans:
        reversed_ids = list(reversed(ids))
        assert await all_ids(scan, None) == ids
        assert await all_ids(scan, "ascending") == ids
        assert await all_ids(scan, "descending") == reversed_ids
        descending = await scan("descending", None)
        continued = await scan("descending", descending["next"])
        assert [item["id"] for item in continued["items"]] == reversed_ids[2:4]
        with pytest.raises(StorageRequestError, match="cursor"):
            await scan("ascending", descending["next"])


async def test_replaces_complete_task_records_and_pages_filtered_task_scans() -> None:
    storage = MemoryStorage()
    root_id = await create_root(storage)
    first_id = await storage.mint_id()
    second_id = await storage.mint_id()
    third_id = await storage.mint_id()
    first = {**pending_task(first_id, root_id), "memos": {"winner": "first"}}
    second = {**pending_task(second_id, root_id), "background": True}
    third = {**pending_task(third_id, root_id), "abortRequested": True}
    await storage.commit(
        [
            {"type": "task", "value": first},
            {"type": "task", "value": second},
            {"type": "task", "value": third},
        ],
        context,
    )
    running = {
        **first,
        "state": {"status": "running", "checkpoint": {"phase": "effect", "attempt": 1}},
        "abortRequested": True,
    }
    await storage.commit([{"type": "task", "value": running}], context)
    assert await storage.task(first_id, context) == running
    terminal = {
        "id": first_id,
        "conversationId": root_id,
        "kind": first["kind"],
        "version": first["version"],
        "input": first["input"],
        "state": {"status": "terminal", "outcome": {"status": "completed", "result": {"entryId": 99}}},
        "background": False,
        "abortRequested": True,
    }
    await storage.commit([{"type": "task", "value": terminal}], context)
    assert await storage.task(first_id, context) == terminal

    pending_page = await storage.scan_tasks({"status": "pending"}, 1, None, context)
    assert [item["id"] for item in pending_page["items"]] == [second_id]
    next_page = await storage.scan_tasks({"status": "pending"}, 1, pending_page["next"], context)
    assert [item["id"] for item in next_page["items"]] == [third_id]
    terminal_page = await storage.scan_tasks(
        {"status": "terminal", "abortRequested": True}, 10, None, context
    )
    assert terminal_page["items"] == [terminal]
    background_page = await storage.scan_tasks({"background": True}, 10, None, context)
    assert [item["id"] for item in background_page["items"]] == [second_id]


async def test_indexes_request_ids_per_conversation_and_replaces_submissions() -> None:
    storage = MemoryStorage()
    root_id = await create_root(storage)
    second_conversation_id = await storage.mint_id()
    await storage.commit([{"type": "conversation", "value": {"id": second_conversation_id}}], context)
    first_id = await storage.mint_id()
    second_id = await storage.mint_id()
    other_id = await storage.mint_id()
    first = {
        "id": first_id,
        "conversationId": root_id,
        "requestId": "same",
        "type": "input",
        "status": "queued",
    }
    second = {
        "id": second_id,
        "conversationId": root_id,
        "requestId": "other",
        "type": "input",
        "status": "queued",
    }
    other = {
        "id": other_id,
        "conversationId": second_conversation_id,
        "requestId": "same",
        "type": "input",
        "status": "queued",
    }
    await storage.commit(
        [
            {"type": "submission", "value": first},
            {"type": "submission", "value": second},
            {"type": "submission", "value": other},
        ],
        context,
    )
    assert await storage.submission_by_request(root_id, "same", context) == first
    assert await storage.submission_by_request(second_conversation_id, "same", context) == other
    placed_second = {**second, "status": "placed", "entry": await storage.mint_id()}
    await storage.commit([{"type": "submission", "value": placed_second}], context)
    assert await storage.submission(second_id, context) == placed_second
    assert await storage.submission_by_request(root_id, "other", context) == placed_second

    async def ids(query: dict[str, Any]) -> list[int]:
        found: list[int] = []
        cursor = None
        while True:
            page = await storage.scan_submissions(query, 1, cursor, context)
            found.extend(item["id"] for item in page["items"])
            if page.get("next") is None:
                return found
            cursor = page["next"]

    assert await ids({}) == [first_id, second_id, other_id]
    assert await ids({"conversationId": root_id}) == [first_id, second_id]
    assert await ids({"status": "queued"}) == [first_id, other_id]
    assert await ids({"status": "placed"}) == [second_id]
    assert await ids({"conversationId": second_conversation_id, "status": "queued"}) == [other_id]
    placed_page = await storage.scan_submissions({"status": "placed"}, 10, None, context)
    assert placed_page["items"] == [placed_second]


async def test_reconstructs_rewindable_documents_and_preserves_half_open_incarnations() -> None:
    storage = MemoryStorage()
    root_id = await create_root(storage)
    first_id = await storage.mint_id()
    first_record: DocumentCreate = {
        "id": first_id,
        "kind": "conversation.notes",
        "scope": {"kind": "conversation", "conversationId": root_id},
        "history": "rewindable",
        "fork": "asOf",
    }
    initial: dict[str, Any] = {"items": ["a"], "nested": {"count": 1}}
    created_at = await storage.commit(
        [
            {
                "type": "document.create",
                "record": first_record,
                "content": {"kind": "base", "version": 1, "value": initial},
            }
        ],
        context,
    )
    appended = ["b"]
    ops = [["p", ["items"], 1, 0, appended], ["s", ["nested", "count"], 2]]
    changed_at = await storage.commit(
        [{"type": "document.change", "id": first_id, "content": {"kind": "delta", "version": 1, "ops": ops}}],
        context,
    )
    initial["items"].append("caller mutation")
    appended.append("caller mutation")
    original = await storage.document(first_id, created_at, context)
    assert original is not None
    assert original["value"] == {"items": ["a"], "nested": {"count": 1}}
    assert original["deltasSinceBase"] == 0
    changed = await storage.document(first_id, changed_at, context)
    assert changed is not None
    assert changed["value"] == {"items": ["a", "b"], "nested": {"count": 2}}
    assert changed["deltasSinceBase"] == 1
    changed["value"]["items"].append("read mutation")
    current = await storage.document(first_id, "current", context)
    assert current is not None and current["value"] == {"items": ["a", "b"], "nested": {"count": 2}}

    checkpoint_at = await storage.commit(
        [
            {
                "type": "document.change",
                "id": first_id,
                "content": {
                    "kind": "base",
                    "version": 2,
                    "value": {"items": ["checkpoint"], "nested": {"count": 3}},
                },
            }
        ],
        context,
    )
    replaced_at = await storage.commit(
        [
            {
                "type": "document.change",
                "id": first_id,
                "content": {
                    "kind": "delta",
                    "version": 2,
                    "ops": [["r", {"items": ["replacement"], "nested": {"count": 4}}]],
                },
            }
        ],
        context,
    )
    old = await storage.document(first_id, changed_at, context)
    assert old is not None and old["version"] == 1
    checkpoint = await storage.document(first_id, checkpoint_at, context)
    assert checkpoint is not None and checkpoint["version"] == 2
    assert checkpoint["value"] == {"items": ["checkpoint"], "nested": {"count": 3}}
    assert checkpoint["deltasSinceBase"] == 0
    replaced = await storage.document(first_id, replaced_at, context)
    assert replaced is not None and replaced["deltasSinceBase"] == 1

    second_id = await storage.mint_id()
    retired_at = await storage.commit(
        [
            {
                "type": "document.create",
                "record": {**first_record, "id": second_id},
                "content": {"kind": "base", "version": 1, "value": {"items": ["new"]}},
            },
            {"type": "document.retire", "id": first_id},
            {
                "type": "document.change",
                "id": first_id,
                "content": {"kind": "delta", "version": 2, "ops": [["s", ["retiring"], True]]},
            },
        ],
        context,
    )
    address = {"kind": first_record["kind"], "scope": first_record["scope"]}
    historical = await storage.find_document(address, changed_at, context)
    assert historical is not None and historical["id"] == first_id
    replacement = await storage.find_document(address, retired_at, context)
    assert replacement is not None and replacement["id"] == second_id
    assert await storage.document(first_id, retired_at, context) is None
    current = await storage.document(second_id, "current", context)
    assert current is not None and current["value"] == {"items": ["new"]}


async def test_copies_stored_document_bases_independently_and_rejects_ambiguous_sources() -> None:
    storage = MemoryStorage()
    root_id = await create_root(storage)
    child_id = await storage.mint_id()
    second_child_id = await storage.mint_id()
    await storage.commit(
        [
            {"type": "conversation", "value": {"id": child_id}},
            {"type": "conversation", "value": {"id": second_child_id}},
        ],
        context,
    )
    source_id = await storage.mint_id()
    source_record: DocumentCreate = {
        "id": source_id,
        "kind": "copy.source",
        "scope": {"kind": "conversation", "conversationId": root_id},
        "history": "rewindable",
        "fork": "asOf",
    }
    created_at = await storage.commit(
        [
            {
                "type": "document.create",
                "record": source_record,
                "content": {"kind": "base", "version": 2, "value": {"count": 1, "rows": [{"value": "base"}]}},
            }
        ],
        context,
    )
    await storage.commit(
        [
            {
                "type": "document.change",
                "id": source_id,
                "content": {
                    "kind": "delta",
                    "version": 2,
                    "ops": [["s", ["count"], 2], ["p", ["rows"], 1, 0, [{"value": "current"}]]],
                },
            }
        ],
        context,
    )
    historical_copy_id = await storage.mint_id()
    current_copy_id = await storage.mint_id()

    def child_record(id: int, conversation_id: int) -> DocumentCreate:
        return cast(
            DocumentCreate,
            {
                "id": id,
                "kind": source_record["kind"],
                "scope": {"kind": "conversation", "conversationId": conversation_id},
                "history": "rewindable",
                "fork": "asOf",
            },
        )

    await storage.commit(
        [
            {
                "type": "document.copy",
                "record": child_record(historical_copy_id, child_id),
                "source": {"id": source_id, "at": created_at},
            },
            {
                "type": "document.copy",
                "record": child_record(current_copy_id, second_child_id),
                "source": {"id": source_id, "at": "current"},
            },
        ],
        context,
    )
    historical = await storage.document(historical_copy_id, "current", context)
    assert historical is not None and historical["value"] == {"count": 1, "rows": [{"value": "base"}]}
    current = await storage.document(current_copy_id, "current", context)
    assert current is not None and current["value"] == {
        "count": 2,
        "rows": [{"value": "base"}, {"value": "current"}],
    }

    conflict_id = await storage.mint_id()
    with pytest.raises(ValueError):
        await storage.commit(
            [
                {
                    "type": "document.copy",
                    "record": child_record(conflict_id, second_child_id),
                    "source": {"id": current_copy_id, "at": "current"},
                },
                {"type": "document.retire", "id": current_copy_id},
            ],
            context,
        )
    assert await storage.document(conflict_id, "current", context) is None
    assert (await storage.document(current_copy_id, "current", context))["value"] == {
        "count": 2,
        "rows": [{"value": "base"}, {"value": "current"}],
    }

    mismatch_id = await storage.mint_id()
    with pytest.raises(ValueError):
        await storage.commit(
            [
                {
                    "type": "document.copy",
                    "record": {
                        **child_record(mismatch_id, second_child_id),
                        "kind": "copy.mismatch",
                    },
                    "source": {"id": current_copy_id, "at": "current"},
                }
            ],
            context,
        )


async def test_uses_bases_for_version_transitions_and_rejects_current_only_history() -> None:
    storage = MemoryStorage()
    await create_root(storage)
    id = await storage.mint_id()
    record: DocumentCreate = {"id": id, "kind": "session.settings", "scope": {"kind": "session"}}
    await storage.commit(
        [
            {
                "type": "document.create",
                "record": record,
                "content": {"kind": "base", "version": 1, "value": {"count": 1}},
            }
        ],
        context,
    )
    await storage.commit(
        [
            {
                "type": "document.change",
                "id": id,
                "content": {"kind": "delta", "version": 1, "ops": [["s", ["count"], 2]]},
            }
        ],
        context,
    )
    migrated_at = await storage.commit(
        [
            {
                "type": "document.change",
                "id": id,
                "content": {"kind": "base", "version": 2, "value": {"count": 3}},
            }
        ],
        context,
    )
    current = await storage.document(id, "current", context)
    assert current is not None and current["version"] == 2 and current["value"] == {"count": 3}
    with pytest.raises(StorageRequestError, match="does not retain historical content"):
        await storage.document(id, migrated_at, context)
    with pytest.raises(ValueError, match="version transition requires a base"):
        await storage.commit(
            [
                {
                    "type": "document.change",
                    "id": id,
                    "content": {"kind": "delta", "version": 1, "ops": [["s", ["count"], 4]]},
                }
            ],
            context,
        )
    await storage.commit([{"type": "document.retire", "id": id}], context)
    assert await storage.document(id, "current", context) is None


async def test_create_plus_retire_has_an_empty_lifetime_and_failures_are_atomic() -> None:
    storage = MemoryStorage()
    root_id = await create_root(storage)
    first_id = await storage.mint_id()
    second_id = await storage.mint_id()
    record: DocumentCreate = {"id": first_id, "kind": "lifecycle", "scope": {"kind": "session"}}
    created_at = await storage.commit(
        [
            {
                "type": "document.create",
                "record": record,
                "content": {"kind": "base", "version": 1, "value": {"value": 1}},
            }
        ],
        context,
    )
    with pytest.raises(ValueError, match="already has a current incarnation"):
        await storage.commit(
            [
                {
                    "type": "document.create",
                    "record": {**record, "id": second_id},
                    "content": {"kind": "base", "version": 1, "value": {"value": 2}},
                },
                {
                    "type": "document.change",
                    "id": first_id,
                    "content": {"kind": "delta", "version": 1, "ops": []},
                },
            ],
            context,
        )
    first = await storage.document(first_id, "current", context)
    assert first is not None and first["value"] == {"value": 1}
    assert await storage.document(second_id, "current", context) is None

    empty_id = await storage.mint_id()
    empty_at = await storage.commit(
        [
            {
                "type": "document.create",
                "record": {
                    "id": empty_id,
                    "kind": record["kind"],
                    "key": "empty",
                    "scope": {"kind": "conversation", "conversationId": root_id},
                    "history": "rewindable",
                    "fork": "initial",
                },
                "content": {"kind": "base", "version": 1, "value": {}},
            },
            {"type": "document.retire", "id": empty_id},
        ],
        context,
    )
    assert await storage.document(empty_id, "current", context) is None
    assert await storage.document(empty_id, empty_at, context) is None
    assert empty_at > created_at


async def test_keeps_one_global_id_namespace_and_rejects_exhausted_minting() -> None:
    storage = MemoryStorage()
    root_id = await create_root(storage)
    await storage.commit(
        [
            {"type": "entry", "value": entry(await storage.mint_id(), root_id)},
        ],
        context,
    )
    storage._next_id = 9_007_199_254_740_992
    with pytest.raises(ValueError, match="ID space is exhausted"):
        await storage.mint_id()


async def test_rejects_every_operation_after_close() -> None:
    storage = MemoryStorage()
    await storage.close(context)
    with pytest.raises(RuntimeError, match="closed"):
        await storage.mint_id()
    with pytest.raises(RuntimeError, match="closed"):
        await storage.commit([], context)
    with pytest.raises(RuntimeError, match="closed"):
        await storage.conversation(ROOT_CONVERSATION_ID, context)
